import shutil
import subprocess

import rich.console
import typer

app = typer.Typer(help='offsets-db-data-orcli')
console = rich.console.Console()


@app.command()
def install(
    move: bool = typer.Option(
        False,
        help='If True, move the downloaded file to the specified destination.',
        show_default=True,
    ),
    destination: str = typer.Option(
        '',
        help='The destination path to move the downloaded file to.',
        show_default=True,
    ),
):
    """
    Install orcli from GitHub.
    """
    # Download orcli from GitHub
    subprocess.run(
        ['wget', 'https://github.com/opencultureconsulting/orcli/raw/main/orcli'], check=True
    )
    # Make the file executable
    subprocess.run(['chmod', '+x', 'orcli'], check=True)
    # Optionally move the file to the specified destination
    if move:
        subprocess.run(['sudo', 'mv', 'orcli', destination], check=True)


@app.command()
def run(
    args: list[str] = typer.Argument(help='The arguments to pass to orcli.'),
    binary_path: str | None = typer.Option(
        None, help='The path to the orcli binary.', show_default=True
    ),
):
    """
    Run orcli with the specified arguments.
    """
    if binary_path is None:
        binary_path = shutil.which('orcli')
    if binary_path is None:
        typer.echo('orcli not found. Please install orcli first.')
        raise typer.Exit(1)

    command = [binary_path] + list(args)
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    console.print(result.stdout)
    if result.stderr:
        console.print(result.stderr)
        raise typer.Exit(result.returncode)
    return result.stdout


def main():
    app()


if __name__ == '__main__':
    main()
