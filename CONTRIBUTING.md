# Contributing

## Dev Container / Github Codespaces
The easiest way to start contributing is via our Dev Container. This container works both locally in Visual Studio Code as well as [Github Codespaces](https://github.com/features/codespaces). To open the project in vscode you will need the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers). For codespaces you will need to [create a new codespace](https://codespace.new/Mooncake-Labs/pg_mooncake).

With the extension installed you can run the following from the `Command Palette` to get started
```
> Dev Containers: Clone Repository in Container Volume...
```

In the subsequent popup paste the url to the repo and hit enter.
```
https://github.com/Mooncake-Labs/pg_mooncake
```

This will create an isolated Workspace in vscode, including all tools required to build, test and run the `pg_mooncake` extension.

Now you can compile and install the extension
```bash
git submodule update --init --recursive
make debug
make install
```
Then, connect to Postgres using `psql`.

Once connected, you can enable the extension and begin development:
```sql
CREATE EXTENSION pg_mooncake;
```

### Debugging
1. Identify the Process: Take note of the pid that appears in your psql prompt. For example:
```sql
mooncake (pid: 1219) =#
```
This pid (1219 in this case) indicates the process that you should attach the debugger to.

If PID is not displayed on the `psql` screen (depending on `psql` version), you could check via
```sql
SELECT * FROM pg_backend_pid();
```

2. Start Debugging: Press F5 to start debugging. When prompted, you'll need to attach the debugger to the appropriate Postgres process.

3. Set Breakpoints and Debug: With the debugger attached, you can set breakpoints within the code. This allows you to step through the code execution, inspect variables, and fully debug the Postgres instance running in your container.

#### Tips on debug build and gdb
- `binutils` with version 2.34/2.35 is broken, please make sure you're not dealing with these two versions
  + [issue report](https://sourceware.org/bugzilla/show_bug.cgi?id=26548)

## Testing
Tests use standard regression tests for Postgres extensions. To run tests, run `make installcheck`.

## Formatting
Ensure to run `make format` to format the code.
