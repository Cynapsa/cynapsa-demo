package sessionkernel

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	coreclock "github.com/Cynapsa/cynapsagocore/internal/clock"
	"github.com/Cynapsa/cynapsagocore/internal/enrollment"
	"github.com/Cynapsa/cynapsagocore/internal/model"
	"github.com/Cynapsa/cynapsagocore/internal/protocol"
	coreruntime "github.com/Cynapsa/cynapsagocore/internal/runtime"
)

const (
	maxEnrollmentAttempts  = 3
	enrollmentRetryDelay   = 50 * time.Millisecond
	maxEnrollmentMeshID    = 255
	credentialRenewalLead  = 60 * time.Second
	credentialSafetyMargin = 5 * time.Minute
	minimumRenewalInterval = 5 * time.Minute
	maximumRenewalBackoff  = 30 * time.Minute
)

type profileCredentialSource struct {
	store     enrollment.ProfileStore
	provider  enrollment.Provider
	clock     coreclock.Clock
	random    io.Reader
	profileID string
	meshID    string
	// installationID binds this source to the graph that created it. A later
	// force enrollment must never lend its replacement JWT to an old session.
	installationID string

	mu          sync.Mutex
	renewMu     sync.Mutex
	denied      bool
	nextAttempt time.Time
}

func (controller *SessionController) tokenAuthHandler(personality Personality) coreruntime.Handler {
	return func(ctx context.Context, services coreruntime.Services, command model.Command) (model.Result, error) {
		args, ok := operationArgs[model.TokenAuthArgs](command.Args)
		if !ok || len(args.MeshID) > maxEnrollmentMeshID || protocol.ValidateMeshID(args.MeshID) != nil || !enrollment.ValidTokenBody(args.Token) || !enrollment.ValidProfileID(args.ProfileID) {
			return providerFailure(command, "auth", &ProviderError{Code: ProviderAuthenticationRejected}), nil
		}
		operation, cancel, failure := controller.beginAuthentication(ctx)
		if failure != nil {
			return providerFailure(command, "auth", failure), nil
		}
		defer controller.finishSelection()
		defer cancel()
		if args.ForceEnroll {
			return controller.forceTokenAuthentication(operation, services, command, personality, args), nil
		}

		bundle, usableUntil, source, prepare, failure := controller.acquireProfileCredential(operation, args.Token, args.ProfileID, args.MeshID)
		if failure != nil {
			return providerFailure(command, "auth", failure), nil
		}
		defer bundle.Clear()
		password := append([]byte(nil), bundle.AccessToken...)
		defer clear(password)
		resource := ""
		if bundle.Version == "e2" {
			resource = bundle.SessionResource
		}
		result := controller.completeAuthentication(operation, services, command, Authentication{
			MeshEndpoint: bundle.MeshEndpoint, Username: bundle.Username, Password: password,
			MeshID: bundle.MeshID, AgentInstanceID: bundle.InstallationID,
			SessionResource: resource, TokenAuthentication: true, CredentialUsableUntil: usableUntil,
			CredentialSource: source, SessionExpiryMode: bundle.SessionExpiryMode,
			ProfileID: args.ProfileID, OfflineStartDeadline: usableUntil.Add(-credentialSafetyMargin),
			OfflineColdStartTargetSeconds: bundle.OfflineColdStartTargetSeconds,
			OfflineTargetSatisfied:        offlineTargetSatisfied(bundle, usableUntil, controller.clock.Now().UTC()),
			PolicyRevision:                bundle.PolicyRevision, PreparationStatus: preparationStatus(bundle, prepare),
		}, "", personality)
		if result.Err == nil && source != nil {
			controller.startCredentialWorker(source, prepare)
		}
		return result, nil
	}
}

// Force enrollment retains the prior active profile until a new installation
// has enrolled and authenticated. The pending profile is durable so a failed
// network login can retry the same server-side installation instead of
// consuming another enrollment-grant slot.
func (controller *SessionController) forceTokenAuthentication(ctx context.Context, services coreruntime.Services, command model.Command, personality Personality, args model.TokenAuthArgs) model.Result {
	store, ok := controller.enrollmentState.(enrollment.ProfileStore)
	if !ok {
		return providerFailure(command, "auth", &ProviderError{Code: ProviderUnavailable})
	}
	// A force attempt owns the pending installation until it either publishes
	// the replacement or fails. The separate per-profile lease rejects a
	// concurrent attempt without holding the ordinary profile writer lock
	// across enrollment or transport authentication.
	release, acquired, err := store.TryLockForceProfile(ctx, args.ProfileID)
	if err != nil {
		return providerFailure(command, "auth", &ProviderError{Code: ProviderInternal})
	}
	if !acquired {
		return providerFailure(command, "auth", &ProviderError{Code: ProviderRejected})
	}
	defer release()
	active, found, err := store.LoadProfile(ctx, args.ProfileID)
	if err != nil {
		return providerFailure(command, "auth", &ProviderError{Code: ProviderInternal})
	}
	defer active.Clear()
	priorInstallationID := ""
	if found {
		priorInstallationID = active.InstallationID
	}
	pendingID, err := enrollment.ForcePendingProfileID(args.ProfileID)
	if err != nil {
		return providerFailure(command, "auth", &ProviderError{Code: ProviderInternal})
	}
	pending, failure := controller.reserveForcePendingProfile(ctx, store, pendingID, priorInstallationID, active.AgentID)
	if failure != nil {
		return providerFailure(command, "auth", failure)
	}
	defer pending.Clear()
	bundle, failure := controller.enrollProfileInstallation(ctx, args.Token, pending, args.MeshID)
	if failure != nil {
		return providerFailure(command, "auth", failure)
	}
	defer bundle.Clear()
	if found && active.AgentID != "" && active.AgentID != bundle.AgentID {
		// The supplied token belongs to a different logical agent. This
		// pending installation may already be bound to that agent at the
		// enrollment service, so it cannot be reused for the next token.
		if _, rotateFailure := controller.rotateForcePendingProfile(ctx, store, pendingID, pending.InstallationID); rotateFailure != nil {
			return providerFailure(command, "auth", rotateFailure)
		}
		return providerFailure(command, "auth", &ProviderError{Code: ProviderAuthenticationRejected})
	}
	usableUntil, failure := persistProfileCredential(ctx, store, controller.clock.Now().UTC(), &pending, bundle)
	if failure != nil {
		return providerFailure(command, "auth", failure)
	}
	source := &profileCredentialSource{store: store, provider: controller.enroll, clock: controller.clock,
		random: controller.random, profileID: pendingID, meshID: args.MeshID, installationID: pending.InstallationID}
	password := append([]byte(nil), bundle.AccessToken...)
	defer clear(password)
	resource := ""
	if bundle.Version == "e2" {
		resource = bundle.SessionResource
	}
	var promotedByThisAttempt bool
	var displaced enrollment.Profile
	defer displaced.Clear()
	result := controller.completeAuthenticationWithCommit(ctx, services, command, Authentication{
		MeshEndpoint: bundle.MeshEndpoint, Username: bundle.Username, Password: password,
		MeshID: bundle.MeshID, AgentInstanceID: bundle.InstallationID,
		SessionResource: resource, TokenAuthentication: true, CredentialUsableUntil: usableUntil,
		CredentialSource: source, SessionExpiryMode: bundle.SessionExpiryMode,
		ProfileID: args.ProfileID, OfflineStartDeadline: usableUntil.Add(-credentialSafetyMargin),
		OfflineColdStartTargetSeconds: bundle.OfflineColdStartTargetSeconds,
		OfflineTargetSatisfied:        offlineTargetSatisfied(bundle, usableUntil, controller.clock.Now().UTC()),
		PolicyRevision:                bundle.PolicyRevision, PreparationStatus: preparationStatus(bundle, false),
	}, "", personality, func() *ProviderError {
		var failure *ProviderError
		displaced, promotedByThisAttempt, failure = promoteForceProfile(ctx, store, active, found, priorInstallationID, pending, args.ProfileID)
		if failure != nil {
			return failure
		}
		// The first connection reads pending; subsequent reconnects use the
		// newly promoted active profile.
		source.mu.Lock()
		source.profileID = args.ProfileID
		source.mu.Unlock()
		return nil
	}, func() *ProviderError {
		if !promotedByThisAttempt {
			return nil
		}
		cleanup, cancel := context.WithTimeout(context.Background(), controller.profile.ReconnectOperationTimeout)
		defer cancel()
		failure := rollbackForceProfile(cleanup, store, displaced, displaced.InstallationID != "", pending, args.ProfileID)
		source.mu.Lock()
		source.profileID = pendingID
		source.mu.Unlock()
		return failure
	})
	if result.Err == nil {
		controller.startCredentialWorker(source, false)
	}
	return result
}

func (controller *SessionController) reserveForcePendingProfile(ctx context.Context, store enrollment.ProfileStore, pendingID, priorInstallationID, priorAgentID string) (enrollment.Profile, *ProviderError) {
	for attempt := 0; attempt < 4; attempt++ {
		candidate, failure := controller.newForcePendingProfile(pendingID)
		if failure != nil {
			return enrollment.Profile{}, failure
		}
		pending, _, err := store.LoadOrCreateProfile(ctx, candidate)
		candidate.Clear()
		if err != nil {
			return enrollment.Profile{}, &ProviderError{Code: ProviderInternal}
		}
		if pending.InstallationID != priorInstallationID && (priorAgentID == "" || pending.AgentID == "" || pending.AgentID == priorAgentID) {
			return pending, nil
		}
		installedID := pending.InstallationID
		pending.Clear()
		if _, failure := controller.rotateForcePendingProfile(ctx, store, pendingID, installedID); failure != nil {
			return enrollment.Profile{}, failure
		}
	}
	return enrollment.Profile{}, &ProviderError{Code: ProviderUnavailable}
}

func (controller *SessionController) newForcePendingProfile(profileID string) (enrollment.Profile, *ProviderError) {
	installationID, secret, err := enrollment.GenerateInstallation(controller.random)
	if err != nil {
		return enrollment.Profile{}, &ProviderError{Code: ProviderInternal}
	}
	return enrollment.Profile{Version: enrollment.ProfileStateVersion, ProfileID: profileID,
		InstallationID: installationID, InstallationSecret: secret,
		Credentials: make(map[string]enrollment.Credential)}, nil
}

func (controller *SessionController) rotateForcePendingProfile(ctx context.Context, store enrollment.ProfileStore, pendingID, installedID string) (bool, *ProviderError) {
	replacement, failure := controller.newForcePendingProfile(pendingID)
	if failure != nil {
		return false, failure
	}
	defer replacement.Clear()
	swapped, err := store.CompareAndSwapProfile(ctx, pendingID, installedID, replacement)
	if err != nil {
		return false, &ProviderError{Code: ProviderInternal}
	}
	return swapped, nil
}

func promoteForceProfile(ctx context.Context, store enrollment.ProfileStore, old enrollment.Profile, found bool, priorID string, pending enrollment.Profile, profileID string) (enrollment.Profile, bool, *ProviderError) {
	active := pending.Clone()
	active.ProfileID = profileID
	defer active.Clear()
	previous, swapped, err := store.SwapProfileWithPrevious(ctx, profileID, priorID, active)
	if err != nil {
		// A file write can fail after atomic rename (for example at directory
		// sync). Resolve its outcome before reporting a failed authentication.
		// The operation context may already be cancelled, so use bounded local
		// cleanup time to inspect and, if necessary, restore the old profile.
		// If the store captured a prior profile before the uncertain write, use
		// that exact copy rather than the possibly stale initial snapshot.
		rollbackProfile, rollbackFound := old, found
		if previous.InstallationID != "" {
			rollbackProfile, rollbackFound = previous, true
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		current, currentFound, loadErr := store.LoadProfile(cleanup, profileID)
		changedToPending := loadErr == nil && currentFound && current.InstallationID == pending.InstallationID
		current.Clear()
		if changedToPending {
			_ = rollbackForceProfile(cleanup, store, rollbackProfile, rollbackFound, pending, profileID)
		}
		previous.Clear()
		return enrollment.Profile{}, false, &ProviderError{Code: ProviderInternal}
	}
	if !swapped {
		previous.Clear()
		// A changed active profile is a conflicting mutation, not our success.
		return enrollment.Profile{}, false, &ProviderError{Code: ProviderInternal}
	}
	return previous, true, nil
}

func rollbackForceProfile(ctx context.Context, store enrollment.ProfileStore, old enrollment.Profile, found bool, pending enrollment.Profile, profileID string) *ProviderError {
	for attempt := 0; attempt < 2; attempt++ {
		var swapped bool
		if found {
			replacement := old.Clone()
			// A post-rename error can still mean rollback succeeded; resolve
			// every non-success path by loading the resulting active profile.
			swapped, _ = store.CompareAndSwapProfile(ctx, profileID, pending.InstallationID, replacement)
			replacement.Clear()
		} else {
			swapped, _ = store.CompareAndDeleteProfile(ctx, profileID, pending.InstallationID)
		}
		if swapped {
			return nil
		}
		current, currentFound, err := store.LoadProfile(ctx, profileID)
		matchesOld := err == nil && currentFound == found && (!found ||
			(current.InstallationID == old.InstallationID && bytes.Equal(current.InstallationSecret, old.InstallationSecret)))
		stillPending := err == nil && currentFound && current.InstallationID == pending.InstallationID
		current.Clear()
		if matchesOld {
			return nil
		}
		if !stillPending {
			break
		}
	}
	return &ProviderError{Code: ProviderInternal}
}

func (controller *SessionController) installationAuthHandler(personality Personality) coreruntime.Handler {
	return func(ctx context.Context, services coreruntime.Services, command model.Command) (model.Result, error) {
		args, ok := operationArgs[model.InstallationAuthArgs](command.Args)
		if !ok || len(args.MeshID) > maxEnrollmentMeshID || protocol.ValidateMeshID(args.MeshID) != nil || !enrollment.ValidProfileID(args.ProfileID) {
			return providerFailure(command, "auth", &ProviderError{Code: ProviderAuthenticationRejected}), nil
		}
		operation, cancel, failure := controller.beginAuthentication(ctx)
		if failure != nil {
			return providerFailure(command, "auth", failure), nil
		}
		defer controller.finishSelection()
		defer cancel()
		bundle, usableUntil, source, prepare, failure := controller.acquireProfileCredential(operation, nil, args.ProfileID, args.MeshID)
		if failure != nil {
			return providerFailure(command, "auth", failure), nil
		}
		defer bundle.Clear()
		password := append([]byte(nil), bundle.AccessToken...)
		defer clear(password)
		resource := ""
		if bundle.Version == "e2" {
			resource = bundle.SessionResource
		}
		result := controller.completeAuthentication(operation, services, command, Authentication{
			MeshEndpoint: bundle.MeshEndpoint, Username: bundle.Username, Password: password,
			MeshID: bundle.MeshID, AgentInstanceID: bundle.InstallationID, SessionResource: resource,
			TokenAuthentication: true, CredentialUsableUntil: usableUntil, CredentialSource: source,
			SessionExpiryMode: bundle.SessionExpiryMode,
			ProfileID:         args.ProfileID, OfflineStartDeadline: usableUntil.Add(-credentialSafetyMargin),
			OfflineColdStartTargetSeconds: bundle.OfflineColdStartTargetSeconds,
			OfflineTargetSatisfied:        offlineTargetSatisfied(bundle, usableUntil, controller.clock.Now().UTC()),
			PolicyRevision:                bundle.PolicyRevision, PreparationStatus: preparationStatus(bundle, prepare),
		}, "", personality)
		if result.Err == nil {
			controller.startCredentialWorker(source, prepare)
		}
		return result, nil
	}
}

func (controller *SessionController) acquireProfileCredential(ctx context.Context, token []byte, profileID, meshID string) (enrollment.Bundle, time.Time, *profileCredentialSource, bool, *ProviderError) {
	store, ok := controller.enrollmentState.(enrollment.ProfileStore)
	if !ok {
		// The old dependency surface remains usable until injected stores adopt
		// ProfileStore. It cannot support token-free restart or multiple caches.
		if len(token) == 0 {
			return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderUnavailable}
		}
		bundle, until, failure := controller.acquireTokenCredential(ctx, token, meshID)
		return bundle, until, nil, false, failure
	}
	profile, found, err := store.LoadProfile(ctx, profileID)
	if err != nil {
		return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderInternal}
	}
	defer profile.Clear()
	if !found && len(token) > 0 && profileID == enrollment.DefaultProfileID {
		legacy, legacyFound, legacyErr := controller.enrollmentState.Load(ctx, token, meshID)
		if legacyErr != nil {
			return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderInternal}
		}
		if legacyFound {
			candidate := profileFromLegacy(profileID, legacy)
			legacy.Clear()
			var created bool
			profile, created, err = store.LoadOrCreateProfile(ctx, candidate)
			candidate.Clear()
			if err != nil {
				return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderInternal}
			}
			if created {
				if err = store.DeleteLegacy(ctx, token, meshID); err != nil {
					return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderInternal}
				}
			}
			found = true
		}
	}
	if !found {
		if len(token) == 0 {
			return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderAuthenticationRejected}
		}
		installationID, secret, generateErr := enrollment.GenerateInstallation(controller.random)
		if generateErr != nil {
			return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderInternal}
		}
		candidate := enrollment.Profile{Version: enrollment.ProfileStateVersion, ProfileID: profileID, InstallationID: installationID, InstallationSecret: secret, Credentials: make(map[string]enrollment.Credential)}
		profile, _, err = store.LoadOrCreateProfile(ctx, candidate)
		candidate.Clear()
		if err != nil {
			return enrollment.Bundle{}, time.Time{}, nil, false, &ProviderError{Code: ProviderInternal}
		}
	}
	source := &profileCredentialSource{store: store, provider: controller.enroll, clock: controller.clock, random: controller.random, profileID: profileID, meshID: meshID, installationID: profile.InstallationID}
	now := controller.clock.Now().UTC()
	if credential, exists := profile.Credentials[meshID]; exists && now.Before(credential.UsableUntil) {
		return cloneEnrollmentBundle(credential.Bundle), credential.UsableUntil, source, credentialNeedsPreparation(credential, now), nil
	}
	var bundle enrollment.Bundle
	var failure *ProviderError
	if credential, exists := profile.Credentials[meshID]; exists {
		_ = credential
		bundle, failure = controller.renewProfileInstallation(ctx, profile, meshID)
	} else if len(token) > 0 {
		bundle, failure = controller.enrollProfileInstallation(ctx, token, profile, meshID)
	} else {
		bundle, failure = controller.renewProfileInstallation(ctx, profile, meshID)
	}
	if failure != nil {
		return enrollment.Bundle{}, time.Time{}, nil, false, failure
	}
	usableUntil, failure := persistProfileCredential(ctx, store, controller.clock.Now().UTC(), &profile, bundle)
	if failure != nil {
		return enrollment.Bundle{}, time.Time{}, nil, false, failure
	}
	return cloneEnrollmentBundle(bundle), usableUntil, source, credentialNeedsPreparation(profile.Credentials[meshID], controller.clock.Now().UTC()), nil
}

func profileFromLegacy(profileID string, state enrollment.State) enrollment.Profile {
	profile := enrollment.Profile{Version: enrollment.ProfileStateVersion, ProfileID: profileID, InstallationID: state.InstallationID,
		InstallationSecret: append([]byte(nil), state.InstallationSecret...), Credentials: make(map[string]enrollment.Credential)}
	if state.Bundle != nil {
		profile.AgentID = state.Bundle.AgentID
		profile.Credentials[state.MeshID] = enrollment.Credential{Bundle: cloneEnrollmentBundle(*state.Bundle), ReceivedAt: state.ReceivedAt, UsableUntil: state.UsableUntil}
	}
	return profile
}

func persistProfileCredential(ctx context.Context, store enrollment.ProfileStore, receivedAt time.Time, profile *enrollment.Profile, bundle enrollment.Bundle) (time.Time, *ProviderError) {
	usableUntil, err := enrollment.UsableUntil(bundle.AccessToken, receivedAt, bundle.ExpiresIn)
	if err != nil || !receivedAt.Before(usableUntil) || profile == nil || bundle.InstallationID != profile.InstallationID || (profile.AgentID != "" && profile.AgentID != bundle.AgentID) {
		bundle.Clear()
		return time.Time{}, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	if profile.AgentID == "" {
		profile.AgentID = bundle.AgentID
	}
	if profile.Credentials == nil {
		profile.Credentials = make(map[string]enrollment.Credential)
	}
	cached := cloneEnrollmentBundle(bundle)
	clear(cached.Wrapper)
	cached.Wrapper = nil
	cached.Display = ""
	profile.Credentials[bundle.MeshID] = enrollment.Credential{Bundle: cached, ReceivedAt: receivedAt, UsableUntil: usableUntil}
	swapped, err := store.CompareAndSwapProfile(ctx, profile.ProfileID, profile.InstallationID, *profile)
	if err != nil {
		return time.Time{}, &ProviderError{Code: ProviderInternal}
	}
	if !swapped {
		// A force enrollment replaced this installation while enrollment or
		// renewal was in flight. Never resurrect its stale credential cache.
		return time.Time{}, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	return usableUntil, nil
}

func credentialNeedsPreparation(credential enrollment.Credential, now time.Time) bool {
	target := time.Duration(credential.Bundle.OfflineColdStartTargetSeconds) * time.Second
	if target == 0 {
		return credential.UsableUntil.Sub(now) <= credentialRenewalLead
	}
	return credential.UsableUntil.Sub(now) <= target+credentialSafetyMargin
}

func offlineTargetSatisfied(bundle enrollment.Bundle, usableUntil, now time.Time) bool {
	return usableUntil.Add(-credentialSafetyMargin).Sub(now) >= time.Duration(bundle.OfflineColdStartTargetSeconds)*time.Second
}

func preparationStatus(bundle enrollment.Bundle, prepare bool) string {
	if bundle.Version != "e2" {
		return ""
	}
	if bundle.OfflineColdStartTargetSeconds == 0 {
		return "disabled"
	}
	if prepare {
		return "preparing"
	}
	return "ready"
}

func (controller *SessionController) enrollProfileInstallation(ctx context.Context, token []byte, profile enrollment.Profile, meshID string) (enrollment.Bundle, *ProviderError) {
	request := enrollment.Request{Token: append([]byte(nil), token...), MeshID: meshID, InstallationID: profile.InstallationID,
		InstallationSecret: append([]byte(nil), profile.InstallationSecret...)}
	defer request.Clear()
	for attempt := 1; attempt <= maxEnrollmentAttempts; attempt++ {
		bundle, enrollmentFailure, providerFailure := controller.profileEnrollmentAttempt(ctx, request)
		if providerFailure != nil {
			return enrollment.Bundle{}, providerFailure
		}
		if enrollmentFailure == nil {
			return bundle, nil
		}
		bundle.Clear()
		if attempt == maxEnrollmentAttempts || !retryableEnrollmentFailure(ctx, enrollmentFailure) {
			return enrollment.Bundle{}, mapEnrollmentFailure(enrollmentFailure)
		}
		if failure := waitForEnrollmentRetry(ctx, enrollmentRetryDelay*time.Duration(attempt)); failure != nil {
			return enrollment.Bundle{}, failure
		}
	}
	return enrollment.Bundle{}, &ProviderError{Code: ProviderInternal}
}

func (controller *SessionController) renewProfileInstallation(ctx context.Context, profile enrollment.Profile, meshID string) (enrollment.Bundle, *ProviderError) {
	request := enrollment.RenewRequest{MeshID: meshID, InstallationID: profile.InstallationID, InstallationSecret: append([]byte(nil), profile.InstallationSecret...)}
	defer request.Clear()
	for attempt := 1; attempt <= maxEnrollmentAttempts; attempt++ {
		bundle, enrollmentFailure, providerFailure := controller.profileRenewalAttempt(ctx, request)
		if providerFailure != nil {
			return enrollment.Bundle{}, providerFailure
		}
		if enrollmentFailure == nil {
			return bundle, nil
		}
		bundle.Clear()
		if attempt == maxEnrollmentAttempts || !retryableEnrollmentFailure(ctx, enrollmentFailure) {
			return enrollment.Bundle{}, mapEnrollmentFailure(enrollmentFailure)
		}
		if failure := waitForEnrollmentRetry(ctx, enrollmentRetryDelay*time.Duration(attempt)); failure != nil {
			return enrollment.Bundle{}, failure
		}
	}
	return enrollment.Bundle{}, &ProviderError{Code: ProviderInternal}
}

func (controller *SessionController) profileEnrollmentAttempt(ctx context.Context, request enrollment.Request) (enrollment.Bundle, *enrollment.Failure, *ProviderError) {
	provider, ok := controller.enroll.(enrollment.V2Provider)
	if !ok {
		return controller.enrollInstallationAttempt(ctx, request)
	}
	type result struct {
		bundle  enrollment.Bundle
		failure *enrollment.Failure
	}
	handoff := newResultHandoff[result]()
	go func() {
		var output result
		owned := enrollment.Request{Token: append([]byte(nil), request.Token...), MeshID: request.MeshID, InstallationID: request.InstallationID, InstallationSecret: append([]byte(nil), request.InstallationSecret...)}
		defer owned.Clear()
		defer func() {
			if recover() != nil {
				output.bundle.Clear()
				output = result{failure: &enrollment.Failure{Code: enrollment.FailureInvalidResponse}}
			}
			if !handoff.publish(output) {
				output.bundle.Clear()
			}
		}()
		output.bundle, output.failure = provider.EnrollV2(ctx, owned)
	}()
	output, owned := handoff.await(ctx)
	if !owned || ctx.Err() != nil {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, providerContextError(ctx)
	}
	if output.failure != nil {
		return output.bundle, output.failure, nil
	}
	if !enrollment.ValidateBundle(request, output.bundle) || output.bundle.Version != "e2" {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	return output.bundle, nil, nil
}

func (controller *SessionController) profileRenewalAttempt(ctx context.Context, request enrollment.RenewRequest) (enrollment.Bundle, *enrollment.Failure, *ProviderError) {
	provider, ok := controller.enroll.(enrollment.V2Provider)
	if !ok {
		return controller.renewInstallationAttempt(ctx, request)
	}
	type result struct {
		bundle  enrollment.Bundle
		failure *enrollment.Failure
	}
	handoff := newResultHandoff[result]()
	go func() {
		var output result
		owned := enrollment.RenewRequest{MeshID: request.MeshID, InstallationID: request.InstallationID, InstallationSecret: append([]byte(nil), request.InstallationSecret...)}
		defer owned.Clear()
		defer func() {
			if recover() != nil {
				output.bundle.Clear()
				output = result{failure: &enrollment.Failure{Code: enrollment.FailureInvalidResponse}}
			}
			if !handoff.publish(output) {
				output.bundle.Clear()
			}
		}()
		output.bundle, output.failure = provider.RenewV2(ctx, owned)
	}()
	output, owned := handoff.await(ctx)
	if !owned || ctx.Err() != nil {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, providerContextError(ctx)
	}
	if output.failure != nil {
		return output.bundle, output.failure, nil
	}
	if !enrollment.ValidateRenewedBundle(request, output.bundle) || output.bundle.Version != "e2" {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	return output.bundle, nil, nil
}

func (source *profileCredentialSource) Snapshot(ctx context.Context) (CredentialSnapshot, *ProviderError) {
	if source == nil || ctx == nil {
		return CredentialSnapshot{}, &ProviderError{Code: ProviderInternal}
	}
	source.mu.Lock()
	denied := source.denied
	profileID := source.profileID
	source.mu.Unlock()
	if denied {
		return CredentialSnapshot{}, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	profile, found, err := source.store.LoadProfile(ctx, profileID)
	if err != nil || !found {
		return CredentialSnapshot{}, &ProviderError{Code: ProviderInternal}
	}
	defer profile.Clear()
	if profile.InstallationID != source.installationID {
		return CredentialSnapshot{}, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	credential, found := profile.Credentials[source.meshID]
	if !found || !source.clock.Now().UTC().Before(credential.UsableUntil) {
		return CredentialSnapshot{}, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	return CredentialSnapshot{Password: append([]byte(nil), credential.Bundle.AccessToken...), UsableUntil: credential.UsableUntil}, nil
}

func (controller *SessionController) startCredentialWorker(source *profileCredentialSource, immediate bool) {
	if controller == nil || source == nil || controller.lifetime == nil {
		return
	}
	controller.renewalWG.Add(1)
	go func() {
		defer controller.renewalWG.Done()
		backoff := enrollmentRetryDelay
		for {
			delay := source.nextDelay(immediate, backoff)
			immediate = false
			timer := controller.clock.NewTimer(delay)
			select {
			case <-timer.C():
			case <-controller.lifetime.Done():
				timer.Stop()
				return
			}
			renewCtx, cancel := context.WithTimeout(controller.lifetime, controller.profile.ReconnectOperationTimeout)
			renewed, denied := source.renew(renewCtx)
			cancel()
			if denied {
				return
			}
			if renewed {
				backoff = enrollmentRetryDelay
			} else {
				backoff = min(backoff*2, maximumRenewalBackoff)
			}
		}
	}()
}

func (source *profileCredentialSource) nextDelay(immediate bool, backoff time.Duration) time.Duration {
	if immediate {
		source.mu.Lock()
		next := source.nextAttempt
		source.mu.Unlock()
		if next.IsZero() || !next.After(source.clock.Now().UTC()) {
			return 0
		}
		return next.Sub(source.clock.Now().UTC())
	}
	profile, found, err := source.store.LoadProfile(context.Background(), source.profileID)
	if err != nil || !found {
		return backoff
	}
	defer profile.Clear()
	if profile.InstallationID != source.installationID {
		// Promotion is provisional until Runtime publishes ready. A failed
		// publication can restore this installation, so retry later rather
		// than permanently denying the old source.
		return backoff
	}
	credential, found := profile.Credentials[source.meshID]
	if !found {
		return backoff
	}
	now := source.clock.Now().UTC()
	source.mu.Lock()
	nextAttempt := source.nextAttempt
	source.mu.Unlock()
	if credentialNeedsPreparation(credential, now) && !nextAttempt.After(now) {
		delay := backoff
		if jitter, jitterErr := CryptoJitter(source.random, max(backoff/4, time.Nanosecond)); jitterErr == nil {
			delay += jitter
		}
		return delay
	}
	target := time.Duration(credential.Bundle.OfflineColdStartTargetSeconds)*time.Second + credentialSafetyMargin
	if credential.Bundle.OfflineColdStartTargetSeconds == 0 {
		target = credentialRenewalLead
	}
	delay := credential.UsableUntil.Sub(now) - target
	if delay < minimumRenewalInterval {
		delay = minimumRenewalInterval
	}
	jitter, err := CryptoJitter(source.random, min(delay/10, time.Minute))
	if err == nil && jitter < delay {
		delay -= jitter
	}
	if nextAttempt.After(now) && nextAttempt.Sub(now) > delay {
		delay = nextAttempt.Sub(now)
	}
	return delay
}

func (source *profileCredentialSource) renew(ctx context.Context) (bool, bool) {
	source.renewMu.Lock()
	defer source.renewMu.Unlock()
	source.mu.Lock()
	denied := source.denied
	source.mu.Unlock()
	if denied {
		return false, true
	}
	profile, found, err := source.store.LoadProfile(ctx, source.profileID)
	if err != nil || !found {
		return false, false
	}
	defer profile.Clear()
	if profile.InstallationID != source.installationID {
		return false, false
	}
	provider, v2 := source.provider.(enrollment.V2Provider)
	legacy, v1 := source.provider.(enrollment.RenewalProvider)
	if !v2 && !v1 {
		return false, false
	}
	request := enrollment.RenewRequest{MeshID: source.meshID, InstallationID: profile.InstallationID, InstallationSecret: append([]byte(nil), profile.InstallationSecret...)}
	var bundle enrollment.Bundle
	var failure *enrollment.Failure
	type renewalResult struct {
		bundle  enrollment.Bundle
		failure *enrollment.Failure
	}
	result := make(chan renewalResult)
	providerRequest := enrollment.RenewRequest{MeshID: request.MeshID, InstallationID: request.InstallationID, InstallationSecret: append([]byte(nil), request.InstallationSecret...)}
	go func(owned enrollment.RenewRequest) {
		defer owned.Clear()
		var output renewalResult
		if v2 {
			output.bundle, output.failure = provider.RenewV2(ctx, owned)
		} else {
			output.bundle, output.failure = legacy.Renew(ctx, owned)
		}
		select {
		case result <- output:
		case <-ctx.Done():
			output.bundle.Clear()
		}
	}(providerRequest)
	select {
	case output := <-result:
		bundle, failure = output.bundle, output.failure
	case <-ctx.Done():
		request.Clear()
		return false, false
	}
	request.Clear()
	if failure != nil {
		if failure.Code == enrollment.FailureRejected || failure.Code == enrollment.FailureInvalidResponse {
			source.mu.Lock()
			source.denied = true
			source.mu.Unlock()
			return false, true
		}
		return false, false
	}
	defer bundle.Clear()
	if !enrollment.ValidateRenewedBundle(enrollment.RenewRequest{MeshID: source.meshID, InstallationID: profile.InstallationID, InstallationSecret: profile.InstallationSecret}, bundle) || (v2 && bundle.Version != "e2") {
		source.mu.Lock()
		source.denied = true
		source.mu.Unlock()
		return false, true
	}
	_, providerFailure := persistProfileCredential(ctx, source.store, source.clock.Now().UTC(), &profile, bundle)
	if providerFailure != nil && providerFailure.Code == ProviderAuthenticationRejected {
		// A local installation-ID CAS can lose to provisional force
		// promotion. Retry after that transaction settles; only a server
		// rejection above is a terminal credential denial.
		return false, false
	}
	if providerFailure == nil {
		now := source.clock.Now().UTC()
		credential := profile.Credentials[source.meshID]
		nextAttempt := time.Time{}
		if credentialNeedsPreparation(credential, now) {
			cooldown := time.Duration(bundle.ExpiresIn) * time.Second / 2
			if cooldown < minimumRenewalInterval {
				cooldown = minimumRenewalInterval
			}
			nextAttempt = now.Add(cooldown)
		}
		source.mu.Lock()
		source.nextAttempt = nextAttempt
		source.mu.Unlock()
	}
	return providerFailure == nil, false
}

func (controller *SessionController) acquireTokenCredential(ctx context.Context, token []byte, meshID string) (enrollment.Bundle, time.Time, *ProviderError) {
	if controller.enrollmentState == nil {
		return enrollment.Bundle{}, time.Time{}, &ProviderError{Code: ProviderInternal}
	}
	state, found, err := controller.enrollmentState.Load(ctx, token, meshID)
	if err != nil {
		return enrollment.Bundle{}, time.Time{}, &ProviderError{Code: ProviderInternal}
	}
	defer state.Clear()
	if !found {
		installationID, installationSecret, generateErr := enrollment.GenerateInstallation(controller.random)
		if generateErr != nil {
			return enrollment.Bundle{}, time.Time{}, &ProviderError{Code: ProviderInternal}
		}
		state = enrollment.State{Version: 1, MeshID: meshID, InstallationID: installationID, InstallationSecret: installationSecret}
		if err = controller.enrollmentState.Save(ctx, token, state); err != nil {
			return enrollment.Bundle{}, time.Time{}, &ProviderError{Code: ProviderInternal}
		}
	}

	now := controller.clock.Now().UTC()
	if state.Bundle != nil && now.Add(credentialRenewalLead).Before(state.UsableUntil) {
		return cloneEnrollmentBundle(*state.Bundle), state.UsableUntil, nil
	}
	if state.Bundle != nil {
		bundle, failure := controller.renewInstallation(ctx, enrollment.RenewRequest{
			MeshID: meshID, InstallationID: state.InstallationID,
			InstallationSecret: append([]byte(nil), state.InstallationSecret...),
		})
		if failure == nil {
			return controller.persistCredential(ctx, token, &state, bundle)
		}
		if (failure.Code == ProviderUnavailable || failure.Code == ProviderDeadline) && controller.clock.Now().UTC().Before(state.UsableUntil) {
			return cloneEnrollmentBundle(*state.Bundle), state.UsableUntil, nil
		}
		return enrollment.Bundle{}, time.Time{}, failure
	}

	request := enrollment.Request{
		Token: append([]byte(nil), token...), MeshID: meshID,
		InstallationID: state.InstallationID, InstallationSecret: append([]byte(nil), state.InstallationSecret...),
	}
	defer request.Clear()
	bundle, failure := controller.enrollInstallation(ctx, request)
	if failure != nil {
		return enrollment.Bundle{}, time.Time{}, failure
	}
	return controller.persistCredential(ctx, token, &state, bundle)
}

func (controller *SessionController) persistCredential(ctx context.Context, token []byte, state *enrollment.State, bundle enrollment.Bundle) (enrollment.Bundle, time.Time, *ProviderError) {
	receivedAt := controller.clock.Now().UTC()
	usableUntil, err := enrollment.UsableUntil(bundle.AccessToken, receivedAt, bundle.ExpiresIn)
	if err != nil || !receivedAt.Before(usableUntil) {
		bundle.Clear()
		return enrollment.Bundle{}, time.Time{}, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	state.Bundle = &bundle
	state.ReceivedAt = receivedAt
	state.UsableUntil = usableUntil
	if err = controller.enrollmentState.Save(ctx, token, *state); err != nil {
		bundle.Clear()
		state.Bundle = nil
		return enrollment.Bundle{}, time.Time{}, &ProviderError{Code: ProviderInternal}
	}
	result := cloneEnrollmentBundle(bundle)
	state.Bundle = nil
	bundle.Clear()
	return result, usableUntil, nil
}

func cloneEnrollmentBundle(bundle enrollment.Bundle) enrollment.Bundle {
	bundle.AccessToken = append([]byte(nil), bundle.AccessToken...)
	bundle.Wrapper = append([]byte(nil), bundle.Wrapper...)
	return bundle
}

func (controller *SessionController) enrollInstallation(ctx context.Context, request enrollment.Request) (enrollment.Bundle, *ProviderError) {
	if controller.enroll == nil {
		return enrollment.Bundle{}, &ProviderError{Code: ProviderUnavailable}
	}
	for attempt := 1; attempt <= maxEnrollmentAttempts; attempt++ {
		bundle, enrollmentFailure, providerFailure := controller.enrollInstallationAttempt(ctx, request)
		if providerFailure != nil {
			return enrollment.Bundle{}, providerFailure
		}
		if enrollmentFailure == nil {
			return bundle, nil
		}
		bundle.Clear()
		if attempt == maxEnrollmentAttempts || !retryableEnrollmentFailure(ctx, enrollmentFailure) {
			return enrollment.Bundle{}, mapEnrollmentFailure(enrollmentFailure)
		}
		if failure := waitForEnrollmentRetry(ctx, enrollmentRetryDelay*time.Duration(attempt)); failure != nil {
			return enrollment.Bundle{}, failure
		}
	}
	return enrollment.Bundle{}, &ProviderError{Code: ProviderInternal}
}

func (controller *SessionController) renewInstallation(ctx context.Context, request enrollment.RenewRequest) (enrollment.Bundle, *ProviderError) {
	defer request.Clear()
	if controller.enroll == nil {
		return enrollment.Bundle{}, &ProviderError{Code: ProviderUnavailable}
	}
	for attempt := 1; attempt <= maxEnrollmentAttempts; attempt++ {
		bundle, enrollmentFailure, providerFailure := controller.renewInstallationAttempt(ctx, request)
		if providerFailure != nil {
			return enrollment.Bundle{}, providerFailure
		}
		if enrollmentFailure == nil {
			return bundle, nil
		}
		bundle.Clear()
		if attempt == maxEnrollmentAttempts || !retryableEnrollmentFailure(ctx, enrollmentFailure) {
			return enrollment.Bundle{}, mapEnrollmentFailure(enrollmentFailure)
		}
		if failure := waitForEnrollmentRetry(ctx, enrollmentRetryDelay*time.Duration(attempt)); failure != nil {
			return enrollment.Bundle{}, failure
		}
	}
	return enrollment.Bundle{}, &ProviderError{Code: ProviderInternal}
}

func (controller *SessionController) renewInstallationAttempt(ctx context.Context, request enrollment.RenewRequest) (enrollment.Bundle, *enrollment.Failure, *ProviderError) {
	type result struct {
		bundle  enrollment.Bundle
		failure *enrollment.Failure
	}
	handoff := newResultHandoff[result]()
	provider, ok := controller.enroll.(enrollment.RenewalProvider)
	if !ok {
		return enrollment.Bundle{}, nil, &ProviderError{Code: ProviderUnavailable}
	}
	go func() {
		var output result
		providerRequest := enrollment.RenewRequest{MeshID: request.MeshID, InstallationID: request.InstallationID,
			InstallationSecret: append([]byte(nil), request.InstallationSecret...)}
		defer providerRequest.Clear()
		defer func() {
			if recover() != nil {
				output.bundle.Clear()
				output = result{failure: &enrollment.Failure{Code: enrollment.FailureInvalidResponse}}
			}
			if !handoff.publish(output) {
				output.bundle.Clear()
			}
		}()
		output.bundle, output.failure = provider.Renew(ctx, providerRequest)
	}()
	output, owned := handoff.await(ctx)
	if !owned {
		return enrollment.Bundle{}, nil, providerContextError(ctx)
	}
	if ctx.Err() != nil {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, providerContextError(ctx)
	}
	if output.failure != nil {
		return output.bundle, output.failure, nil
	}
	if !enrollment.ValidateRenewedBundle(request, output.bundle) {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	return output.bundle, nil, nil
}

func (controller *SessionController) enrollInstallationAttempt(ctx context.Context, request enrollment.Request) (enrollment.Bundle, *enrollment.Failure, *ProviderError) {
	type result struct {
		bundle  enrollment.Bundle
		failure *enrollment.Failure
	}
	handoff := newResultHandoff[result]()
	provider := controller.enroll
	go func() {
		var output result
		providerRequest := enrollment.Request{
			Token: append([]byte(nil), request.Token...), MeshID: request.MeshID,
			InstallationID:     request.InstallationID,
			InstallationSecret: append([]byte(nil), request.InstallationSecret...),
		}
		defer providerRequest.Clear()
		defer func() {
			if recover() != nil {
				output.bundle.Clear()
				output = result{failure: &enrollment.Failure{Code: enrollment.FailureInvalidResponse}}
			}
			if !handoff.publish(output) {
				output.bundle.Clear()
			}
		}()
		output.bundle, output.failure = provider.Enroll(ctx, providerRequest)
	}()
	output, owned := handoff.await(ctx)
	if !owned {
		return enrollment.Bundle{}, nil, providerContextError(ctx)
	}
	if ctx.Err() != nil {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, providerContextError(ctx)
	}
	if output.failure != nil {
		return output.bundle, output.failure, nil
	}
	if !enrollment.ValidateBundle(request, output.bundle) {
		output.bundle.Clear()
		return enrollment.Bundle{}, nil, &ProviderError{Code: ProviderAuthenticationRejected}
	}
	return output.bundle, nil, nil
}

func retryableEnrollmentFailure(ctx context.Context, failure *enrollment.Failure) bool {
	if ctx == nil || ctx.Err() != nil || failure == nil {
		return false
	}
	return failure.Code == enrollment.FailureUnavailable || failure.Code == enrollment.FailureDeadline
}

func waitForEnrollmentRetry(ctx context.Context, delay time.Duration) *ProviderError {
	if ctx == nil || delay <= 0 || ctx.Err() != nil {
		return providerContextError(ctx)
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
		return &ProviderError{Code: ProviderDeadline}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return providerContextError(ctx)
	}
}

func mapEnrollmentFailure(failure *enrollment.Failure) *ProviderError {
	if failure == nil {
		return &ProviderError{Code: ProviderInternal}
	}
	switch failure.Code {
	case enrollment.FailureCancelled:
		return &ProviderError{Code: ProviderCancelled}
	case enrollment.FailureDeadline:
		return &ProviderError{Code: ProviderDeadline}
	case enrollment.FailureRejected, enrollment.FailureInvalidResponse:
		return &ProviderError{Code: ProviderAuthenticationRejected}
	case enrollment.FailureUnavailable:
		return &ProviderError{Code: ProviderUnavailable}
	default:
		return &ProviderError{Code: ProviderInternal}
	}
}
