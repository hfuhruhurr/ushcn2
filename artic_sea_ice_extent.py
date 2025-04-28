

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""Inspiration: https://x.com/TonyClimate/status/1913187952613580825""")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import os
    import seaborn as sns
    from matplotlib import pyplot as plt
    return alt, mo, os, pl, plt, sns


@app.cell
def _():
    source_folder = 'source-data/osisaf-sea-ice-index/'
    return (source_folder,)


@app.function(hide_code=True)
def fetch_osisaf_sie_daily_file(region: str = 'nh', 
                                metric: str = 'sie', 
                                freq: str = 'daily',
                                source_folder: str = 'source-data/osisaf-sea-ice-index/'):
    """
    Fetch an OSI SAF sea ice data file and save it to the specified folder.

    Args:
        region (str): Sea region ('nh' or 'sh'). Defaults to 'nh'.
        metric (str): Metric ('sie' for extent, 'sia' for area). Defaults to 'sie'.
        freq (str): Frequency ('daily' or 'monthly'). Defaults to 'daily'.
        source_folder (str): Folder to save the file. Defaults to 'source-data/osisaf-sea-ice-index/'.

    Returns:
        None

    Raises:
        ftplib.all_errors: If FTP connection or file retrieval fails.
        OSError: If file writing fails.
    """

    import ftplib
    import os

    # Validate inputs
    valid_regions = [
        'bar',
        'beau',
        'bell',
        'chuk',
        'drml',
        'ess',
        'fram',
        'glb',
        'indi',
        'kara',
        'lap',
        'nbar',
        'nh',
        'ross',
        'sh',
        'sval',
        'trol',
        'wedd',
        'wpac',
    ]
    if region not in valid_regions:
        raise ValueError(f"region must be one of {valid_regions}")
    if metric not in ['sie', 'sia']:
        raise ValueError("metric must be 'sie' or 'sia'")
    if freq not in ['daily', 'monthly']:
        raise ValueError("freq must be 'daily' or 'monthly'")

    # Ensure source_folder ends with separator and exists
    source_folder = os.path.join(source_folder, '')
    os.makedirs(source_folder, exist_ok=True)

    file_name = f'osisaf_{region}_{metric}_{freq}.txt'
    try:
        with ftplib.FTP('osisaf.met.no') as ftp:
            ftp.login()  # Anonymous login
            ftp.cwd(f'/prod_test/ice/index/v2p2/{region}/')
            with open(os.path.join(source_folder, file_name), 'wb') as f:
                ftp.retrbinary(f'RETR {file_name}', f.write)
    except ftplib.all_errors as e:
        raise ftplib.error_perm(f"FTP error: {e}")
    except OSError as e:
        raise OSError(f"File writing error: {e}")


@app.cell(hide_code=True)
def _(pl):
    def make_osisaf_df_from_source(source_path: str) -> pl.DataFrame:
        """
        Create a Polars DataFrame from an OSISAF sea ice data file, including creation date.

        Args:
            source_path (str): Path to OSISAF data file (e.g., 'osisaf_nh_sie_daily.txt').

        Returns:
            pl.DataFrame: DataFrame with columns [region, metric, frac_year, year, month, day, area, source, creation_date].

        Raises:
            FileNotFoundError: If source_path does not exist.
            ValueError: If file name format is invalid.
        """
        try:
            # Extract region, metric, and frequency from file name of source path
            file_name = source_path.split('/')[-1]
            parts = file_name.split('_')
            if len(parts) < 4:
                raise ValueError("Invalid file name format")
            agency, region, metric, freq = parts
            freq = freq.split('.')[0]

            # Initialize creation date and data list
            creation_date = None
            data = []

            # Read file to extract the data
            with open(source_path, 'r') as file:
                for line in file:
                    if line.strip() and line[0] == "#":
                        # Extract creation date from comment
                        if "Creation date:" in line:
                            creation_date = line.split("Creation date:")[1].strip()
                    elif line.strip():
                        parts = line.strip().split()
                        data.append([
                            creation_date,
                            region,
                            metric,
                            freq,
                            float(parts[0]) if len(parts) > 0 and parts[0] else None,
                            int(parts[1]) if len(parts) > 1 and parts[1] else None,
                            int(parts[2]) if len(parts) > 2 and parts[2] else None,
                            int(parts[3]) if len(parts) > 3 and parts[3] else None,
                            int(parts[4]) if len(parts) > 4 and parts[4] else None,
                            parts[5] if len(parts) > 5 else None,
                        ])

            return pl.DataFrame(
                data,
                schema=['creation_date', 'region', 'metric', 'freq', 'frac_year', 'year', 'month', 'day', 'area', 'source'],
                orient='row'
            ).with_columns(
                pl.col('creation_date').str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False).cast(pl.Date),
                pl.col('area').replace(-999, None),
            )

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {source_path}")
        except ValueError as e:
            raise ValueError(f"Error processing file: {str(e)}")
    return (make_osisaf_df_from_source,)


@app.cell
def _(make_osisaf_df_from_source, os, pl, source_folder):
    region = 'nh'
    metric = 'sie'
    freq = 'daily'
    input_file = os.path.join(source_folder, f'osisaf_{region}_{metric}_{freq}.txt')
    df = make_osisaf_df_from_source(input_file).select(pl.exclude('frac_year'))
    df
    return df, metric


@app.cell
def _(mo):
    date_picker = mo.ui.date()
    date_picker
    return (date_picker,)


@app.cell(hide_code=True)
def _(alt, date_picker, df, metric, pl):
    month = date_picker.value.month
    day = date_picker.value.day
    metric_name = 'Extent' if metric =='sie' else 'Area'

    df_specific_date = df.filter(pl.col('month') == month, pl.col('day') == day)

    # To allow for missing years to be noticeable on chart
    min_year = df_specific_date['year'].min()
    max_year = df_specific_date['year'].max()
    all_years = range(min_year, max_year + 1)

    (
        df_specific_date
        .plot.bar(
            color=alt.value('red'),
            opacity=alt.value(0.7),
            x=alt.X(
                'year:O', 
                scale=alt.Scale(domain=list(all_years)),
                title='Year'
            ),
            y=alt.Y(
                'area',
                title='SIE (km²)'       
            )
        )
        .properties(
            title=f'Artic Sea Ice {metric_name} (Date: {month}/{day})'
        )
    )
    return (metric_name,)


@app.cell(hide_code=True)
def _(df, metric_name, plt, sns):
    # Recreating https://osisaf-hl.met.no/archive/osisaf/sea-ice-index/v2p2/nh/en/osisaf_nh_sie_monthly-ranks.png
    # Only don't use ranks; use actual values (ranks could mute the deltas)

    # Pivot data in preparation to make a heatmap
    pivot_data = (
        df.pivot(index='year', on='month', values='area', aggregate_function='mean')
    )

    # Convert to pandas and sort
    pivot_df = pivot_data.to_pandas().set_index('year').sort_index()
    month_cols = [str(i) for i in range(1, 13)]  # Ensure months 1 to 12
    pivot_df = pivot_df.reindex(columns=month_cols)

    # Normalize each month independently to [0, 1]
    pivot_normalized = pivot_df.copy()
    for col in pivot_normalized.columns:
        col_min = pivot_normalized[col].min()
        col_max = pivot_normalized[col].max()
        if col_max > col_min:  # Avoid division by zero
            pivot_normalized[col] = (pivot_normalized[col] - col_min) / (col_max - col_min)
        else:
            pivot_normalized[col] = 0  # Handle constant or single-value columns

    # Create heatmap
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(
        pivot_normalized,
        # cmap = 'Blues',
        cmap='RdBu',  # Red (low) to Blue (high)
        annot=pivot_df/1e6,  # Show original values
        fmt='.1f',
        cbar_kws={'label': f'Normalized {metric_name}'},
        yticklabels=pivot_df.index,
        ax=ax
    )
    plt.title(f'Sea Ice {metric_name} by Year and Month\n(Per-Month Normalization)\n')
    ax.set_xlabel('')
    ax.set_ylabel('')
    # Center month ticks and move to top
    ax.set_xticks([i + 0.5 for i in range(12)])
    ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    ax.xaxis.set_ticks_position('top')
    # plt.savefig('sea_ice_heatmap.png')
    plt.show()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
