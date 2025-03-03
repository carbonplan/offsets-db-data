import pathlib
import shutil
import subprocess
import tempfile
import traceback

import requests
import rich.console
import typer

app = typer.Typer(help='offsets-db-data-orcli')
console = rich.console.Console()


@app.command()
def install(
    url: str = typer.Option(
        'https://github.com/opencultureconsulting/orcli/raw/main/orcli',
        help='The URL to download orcli from.',
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

    try:
        tempfile_path = (pathlib.Path(tempfile.mkdtemp()) / 'orcli').as_posix()

        file_path = f'{destination}/orcli' if destination else 'orcli'
        abs_file_path = pathlib.Path(file_path).expanduser().resolve()
        filename = abs_file_path.as_posix()
        # Download orcli from GitHub
        # Download the file with streaming to handle large files.
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise error if the download failed.

        with open(tempfile_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks.
                    f.write(chunk)

        # Make the file executable
        subprocess.run(['chmod', '+x', tempfile_path], check=True)
        subprocess.run(['mv', tempfile_path, destination], check=True)
        console.print(f'orcli installed to {filename}.')

    except Exception as _:
        console.print(f'Error: {traceback.format_exc()}')
        raise typer.Exit(1)


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
