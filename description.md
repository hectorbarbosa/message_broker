## Project

- This is test project called `Message Broker`.
- It has to run a server, which listens on a port, specified by `--port` flag, `8080` by default.

## Architecture

- All code have to be placed in one file.
- No layers required.
- No logger required, except error handling.
- Use only std lib, no custom packages.
- Use as little code, as possible, ~200 rows is fine.

## Queue rules

- FIFO message queue.
- FIFO waiting requests queue, the first receiver gets message first.
- If there is no messages in queue, receiver have to wait until it come, or until timeout, in case timeout is specified.

## Requests examples 

1. PUT /queue?v=message
```bash
curl -XPUT http://127.0.0.1/pet?v=cat
curl -XPUT http://127.0.0.1/pet?v=dog
curl -XPUT http://127.0.0.1/role?v=manager
curl -XPUT http://127.0.0.1/role?v=executive
```
 - Normal response: empty body with 200 (OK) status.
 - Response in case of no v param in request: empty body plus with 400 (Bad Request) status.
 - In case of no message arrived after timeout: empty body plus with 404 (Not Found) status 

 2. GET /queue, assuming that PUT requests above are executed.
```bash
curl http://127.0.0.1/pet => cat
curl http://127.0.0.1/pet => dog
curl http://127.0.0.1/pet => {empty body + status 404 (Not Found)}
curl http://127.0.0.1/pet => {empty body + status 404 (Not Found)}
curl http://127.0.0.1/role => manager
curl http://127.0.0.1/role => executive
curl http://127.0.0.1/role => {empty body + status 404 (Not Found)}
```
- optional timeout with GET requests: `curl http://127.0.0.1/pet?timeout=N`, N in seconds.
