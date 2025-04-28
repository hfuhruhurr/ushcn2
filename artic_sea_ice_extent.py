

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
    import ftplib
    return alt, ftplib, mo, pl


@app.cell
def _():
    source_folder = 'source-data/osisaf-sea-ice-index/'
    return (source_folder,)


@app.cell
def _(ftplib, source_folder):
    def fetch_osisaf_sie_daily_file():
        ftp = ftplib.FTP('osisaf.met.no')
        ftp.login()  # Anonymous login
        ftp.cwd('/prod_test/ice/index/v2p2/nh/')
        with open(f'{source_folder}osisaf_nh_sie_daily.txt', 'wb') as f:
            ftp.retrbinary('RETR osisaf_nh_sie_daily.txt', f.write)
        ftp.quit()
    return


@app.cell
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
def _(make_osisaf_df_from_source, pl, source_folder):
    input_file = f'{source_folder}osisaf_nh_sie_daily.txt'
    sie = (
        make_osisaf_df_from_source(input_file)
        .filter(
            pl.col('metric') == 'sie'
        )
    )
    sie
    return (sie,)


@app.cell
def _(sie):
    sie['source'].value_counts().sort(by='count', descending=True)
    return


@app.cell
def _(mo):
    date_picker = mo.ui.date()
    date_picker
    return (date_picker,)


@app.cell
def _(alt, date_picker, pl, sie):
    month = date_picker.value.month
    day = date_picker.value.day

    sie_specific_date = sie.filter(pl.col('month') == month, pl.col('day') == day)

    # To allow for missing years to be noticeable on chart
    min_year = sie_specific_date['year'].min()
    max_year = sie_specific_date['year'].max()
    all_years = range(min_year, max_year + 1)

    (
        sie_specific_date
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
            title=f'Artic Sea Ice Extent (Date: {month}/{day})'
        )
    )
    return


if __name__ == "__main__":
    app.run()
