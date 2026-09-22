# Run the Cynapsa demo client

The client runs on your laptop and communicates with the deployed demo agents
over Cynapsa. You only need Docker and a Cynapsa enrollment token.

## Requirements

- Git
- Docker Desktop, running before you start the client
- A client agent in the `cynapsa-demo` mesh
- A fresh enrollment token for that client agent

Create the client agent and its enrollment token in the Cynapsa management
portal. Treat the token like a password and do not commit it to Git or paste it
into the repository files.

## Download the client

To download only the client directory instead of the entire repository:

```sh
git clone --filter=blob:none --sparse https://github.com/Cynapsa/cynapsa-demo.git
cd cynapsa-demo
git sparse-checkout set client
cd client
```

If you already cloned the repository, open a terminal in its client directory:

```sh
cd /path/to/cynapsa-demo/client
```

## First run

Start the client:

```sh
./run.sh
```

The script downloads and verifies the correct client image for your computer.
When prompted, paste the enrollment token and press Enter. The token is not
shown while you type.

The client enrolls once and saves its encrypted installation profile in the
Docker volume `cynapsa-demo-client-state`. The temporary enrollment-token file
is removed after the process exits.

After the client starts, enter a question at the prompt. For example:

```text
Where is the nearest gym to 12201 Park Drive, Hollywood, Florida?
```

Enter `quit` to close the client.

## Later runs

The saved installation profile is reused, so later runs do not require another
token:

```sh
cd /path/to/cynapsa-demo/client
./run.sh
```

## Update the client image

To download and run the current published client image again:

```sh
./run.sh --pull
```

## Enroll this laptop again

If the saved installation must be replaced, remove its Docker volume and run
the client with a new enrollment token:

```sh
docker volume rm cynapsa-demo-client-state
./run.sh
```

Removing this volume permanently deletes the client installation state stored
on this laptop. It does not delete the agent in the Cynapsa portal.

## Troubleshooting

- `Docker is required`: install Docker Desktop.
- `Docker is not running`: start Docker Desktop and wait until it is ready.
- Enrollment fails: generate a fresh enrollment token; enrollment tokens are
  intended for first-time use.
- To force a clean image download, run `./run.sh --pull`.

