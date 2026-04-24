import datetime
import io
import json
import zipfile
from collections.abc import Callable

import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from offsets_db_data.common import convert_to_datetime  # noqa: F401
from offsets_db_data.data import catalog
from offsets_db_data.models import project_schema
from offsets_db_data.projects import add_placeholder_projects
from offsets_db_data.registry import get_registry_from_project_id


def validate_data(
    *,
    new_data: pd.DataFrame,
    as_of: datetime.datetime,
    data_type: str,
    quantity_column: str,
    aggregation_func,
) -> None:
    success = False
    for delta_days in [1, 2, 3, 4]:
        try:
            previous_date = (as_of - datetime.timedelta(days=delta_days)).strftime('%Y-%m-%d')
            print(
                f'Validating {data_type} for {as_of.strftime("%Y-%m-%d")} against {previous_date}'
            )
            old_data = catalog[data_type](date=previous_date).read()

            new_quantity = aggregation_func(new_data[quantity_column])
            old_quantity = aggregation_func(old_data[quantity_column])

            print(f'New {data_type}: {new_data.shape} | New {quantity_column}: {new_quantity}')
            print(f'Old {data_type}: {old_data.shape} | Old {quantity_column}: {old_quantity}')

            if new_quantity < old_quantity * 0.99:
                raise ValueError(
                    f'New {data_type}: {new_quantity} (from {as_of.strftime("%Y-%m-%d")})  are less than 99% of old {data_type}: {old_quantity} (from {previous_date})'
                )
            else:
                print(f'New {data_type} are at least 99% of old {data_type}')
                success = True
                break
        except Exception as e:
            print(f'Validation failed for {delta_days} day(s) back: {e}')
            continue

    if not success:
        raise ValueError(
            'Validation failed for either 1, 2, 3, or 4 days back. Please make sure the data is available for either 1, 2, 3 or 4 days back.'
        )


def validate_credits(*, new_credits: pd.DataFrame, as_of: datetime.datetime) -> None:
    validate_data(
        new_data=new_credits,
        as_of=as_of,
        data_type='credits',
        quantity_column='quantity',
        aggregation_func=sum,
    )


def validate_projects(*, new_projects: pd.DataFrame, as_of: datetime.datetime) -> None:
    validate_data(
        new_data=new_projects,
        as_of=as_of,
        data_type='projects',
        quantity_column='project_id',
        aggregation_func=pd.Series.nunique,
    )


def validate(
    *, new_credits: pd.DataFrame, new_projects: pd.DataFrame, as_of: datetime.datetime
) -> None:
    validate_credits(new_credits=new_credits, as_of=as_of)
    validate_projects(new_projects=new_projects, as_of=as_of)


def summarize(
    *,
    credits: pd.DataFrame,
    projects: pd.DataFrame,
    registry_name: str | None = None,
) -> None:
    """
    Summarizes the credits, projects, and project types data.

    Parameters
    ----------
    credits : DataFrame
        The credits data.
    projects : DataFrame
        The projects data.
    registry_name : str, optional
        Name of the specific registry to summarize. If None, summarizes across all registries.

    Returns
    -------
    None
    """
    # Create defensive copies to avoid modifying the original dataframes
    credits = credits if credits.empty else credits.copy()
    projects = projects if projects.empty else projects.copy()

    # Single registry mode
    if registry_name:
        if not projects.empty:
            print(
                f'\n\nRetired and Issued (in Millions) summary for {registry_name}:\n\n'
                f'{projects[["retired", "issued"]].sum() / 1_000_000}\n\n'
                f'{projects.project_id.nunique()} unique projects.\n\n'
            )
        else:
            print(f'No projects found for {registry_name}...')

        if not credits.empty:
            print(
                f'\n\nCredits summary (in Millions) for {registry_name}:\n\n'
                f'{credits.groupby(["transaction_type"])[["quantity"]].sum() / 1_000_000}\n\n'
                f'{credits.shape[0]} total transactions.\n\n'
            )
        else:
            print(f'No credits found for {registry_name}...')

    # Multi-registry mode
    else:
        if not projects.empty:
            print(
                f'Summary Statistics for projects (in Millions):\n'
                f'{projects.groupby(["registry", "is_compliance"])[["retired", "issued"]].sum() / 1_000_000}\n'
            )
        else:
            print('No projects found')

        if not credits.empty:
            credits['registry'] = credits['project_id'].map(get_registry_from_project_id)

            print(
                f'Summary Statistics for credits (in Millions):\n'
                f'{credits.groupby(["registry", "transaction_type"])[["quantity"]].sum() / 1_000_000}\n'
            )
        else:
            print('No credits found')


def to_parquet(
    *,
    credits: pd.DataFrame,
    projects: pd.DataFrame,
    output_paths: dict,
    registry_name: str | None = None,
):
    """
    Write the given DataFrames to Parquet files.

    Parameters
    -----------
    credits : pd.DataFrame
            The DataFrame containing credits data.
    projects : pd.DataFrame
            The DataFrame containing projects data.
    output_paths : dict
            Dictionary containing output file paths.

    registry_name : str, optional
            The name of the registry for logging purposes.
    """
    credits.to_parquet(output_paths['credits'], index=False, compression='gzip', engine='pyarrow')

    prefix = f'{registry_name} ' if registry_name else ''
    print(f'Wrote {prefix} credits to {output_paths["credits"]}...')

    projects.to_parquet(output_paths['projects'], index=False, compression='gzip', engine='pyarrow')
    print(f'Wrote {prefix} projects to {output_paths["projects"]}...')


def _write_parquet_with_metadata(df: pd.DataFrame, generated_at: datetime.datetime) -> bytes:
    """Return parquet bytes for df with generated_at embedded in file-level metadata."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    existing = table.schema.metadata or {}
    table = table.replace_schema_metadata(
        {**existing, b'generated_at': generated_at.isoformat().encode()}
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.read()


def _create_data_zip_buffer(
    *,
    credits: pd.DataFrame,
    projects: pd.DataFrame,
    format_type: str,
    terms_content: str,
    generated_at: datetime.datetime,
) -> io.BytesIO:
    """
    Create a zip buffer containing data files in the specified format with terms of access.

    Parameters
    ----------
    credits : pd.DataFrame
        DataFrame containing credit data.
    projects : pd.DataFrame
        DataFrame containing project data.
    format_type : str
        Format type, either 'csv' or 'parquet'.
    terms_content : str
        Content of the terms of access file.
    generated_at : datetime.datetime
        UTC timestamp when this dataset was generated.

    Returns
    -------
    io.BytesIO
        Buffer containing the zip file.
    """
    zip_buffer = io.BytesIO()
    metadata = json.dumps({'generated_at': generated_at.isoformat()})

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zf:
        zf.writestr('TERMS_OF_DATA_ACCESS.txt', terms_content)
        zf.writestr('metadata.json', metadata)

        if format_type == 'csv':
            with zf.open('credits.csv', 'w') as buffer:
                credits.to_csv(buffer, index=False)
            with zf.open('projects.csv', 'w') as buffer:
                projects.to_csv(buffer, index=False)

        elif format_type == 'parquet':
            zf.writestr('credits.parquet', _write_parquet_with_metadata(credits, generated_at))
            zf.writestr('projects.parquet', _write_parquet_with_metadata(projects, generated_at))

    zip_buffer.seek(0)
    return zip_buffer


def write_latest_production(
    *,
    credits: pd.DataFrame,
    projects: pd.DataFrame,
    bucket: str,
    terms_url: str = 'https://raw.githubusercontent.com/carbonplan/offsets-db-data/refs/heads/main/TERMS_OF_DATA_ACCESS',
):
    """
    Write the latest production data to S3 as zip archives containing CSV and Parquet files.

    Parameters
    ----------
    credits : pd.DataFrame
        DataFrame containing credit data.
    projects : pd.DataFrame
        DataFrame containing project data.
    bucket : str
        S3 bucket path to write the data to.
    terms_url : str, optional
        URL of the terms of access file.
    """
    paths = {
        'csv': f'{bucket}/production/latest/offsets-db.csv.zip',
        'parquet': f'{bucket}/production/latest/offsets-db.parquet.zip',
    }

    generated_at = datetime.datetime.now(tz=datetime.timezone.utc)

    # Get terms content once
    fs = fsspec.filesystem('https')
    terms_content = fs.read_text(terms_url)

    for format_type, path in paths.items():
        # Create zip buffer with data in the appropriate format
        zip_buffer = _create_data_zip_buffer(
            credits=credits,
            projects=projects,
            format_type=format_type,
            terms_content=terms_content,
            generated_at=generated_at,
        )

        # Write buffer to S3
        with fsspec.open(path, 'wb') as f:
            f.write(zip_buffer.getvalue())

        print(f'Wrote {format_type} to {path}...')
        zip_buffer.close()


def transform_registry_data(
    *,
    process_credits_fn: Callable[[], pd.DataFrame],
    process_projects_fn: Callable[[pd.DataFrame], pd.DataFrame],
    output_paths: dict,
    registry_name: str | None = None,
):
    """
    Transform registry data by processing credits and projects, then writing to parquet files.

    Parameters
    ----------
    process_credits_fn : callable
        Function that returns processed credits DataFrame
    process_projects_fn : callable
        Function that takes a credits DataFrame and returns processed projects DataFrame
    output_paths : dict
        Dictionary containing output file paths for 'credits' and 'projects'
    registry_name : str, optional
        Name of the registry for logging purposes
    """
    # Process credits
    credits = process_credits_fn()
    if registry_name:
        print(f'credits for {registry_name}: {credits.head()}')
    else:
        print(f'processed credits: {credits.head()}')

    # Process projects
    projects = process_projects_fn(credits=credits)
    if registry_name:
        print(f'projects for {registry_name}: {projects.head()}')
    else:
        print(f'processed projects: {projects.head()}')

    projects = (
        add_placeholder_projects(projects=projects, credits=credits)
        .convert_to_datetime(columns=['first_issuance_at', 'first_retirement_at'])
        .validate(schema=project_schema)
    )

    # Summarize data
    summarize(credits=credits, projects=projects, registry_name=registry_name)

    # Write to parquet files
    to_parquet(
        credits=credits,
        projects=projects,
        output_paths=output_paths,
        registry_name=registry_name,
    )

    return credits, projects
