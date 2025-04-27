

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
    return alt, mo, pl


@app.cell
def _():
    input_file = 'source-data/osisaf-sea-ice-index/osisaf_nh_sie_daily.txt'

    # Read the data from the file
    data = []
    with open(input_file, 'r') as file:
        for line in file:
            if line[0] != "#":
                # Split the line by whitespace and strip any extra spaces
                parts = line.strip().split()

                # Convert to appropriate types and handle missing values
                decimal_date = float(parts[0]) if len(parts) > 0 else None
                year = int(parts[1]) if len(parts) > 1 else None
                month = int(parts[2]) if len(parts) > 2 else None
                day = int(parts[3]) if len(parts) > 3 else None
                sea_ice_extent = int(parts[4]) if len(parts) > 4 else None
                method = parts[5] if len(parts) > 5 else None

                # Append the processed data to the list
                if len(parts) > 0:
                    data.append([decimal_date, year, month, day, sea_ice_extent, method])
    return (data,)


@app.cell
def _(data, pl):
    sie = (
        pl.DataFrame(data, schema=['decimal_date', 'year', 'month', 'day', 'sea_ice_extent', 'method'], orient='row')
        .with_columns(
            pl.when(pl.col('sea_ice_extent') == -999)
            .then(None)  # Replace -999 with NULL
            .otherwise(pl.col('sea_ice_extent'))  # Keep original value if not -999
            .alias('sea_ice_extent')  # Assign back to column
        )
    )
    sie
    return (sie,)


@app.cell
def _(sie):
    sie['method'].value_counts()
    return


@app.cell
def _(alt, pl, sie):
    sie_4_17 = sie.filter(pl.col('month') == 4, pl.col('day') == 17)

    # To allow for missing years to be noticeable on chart
    min_year = sie_4_17['year'].min()
    max_year = sie_4_17['year'].max()
    all_years = range(min_year, max_year + 1)

    (
        sie_4_17
        .plot.bar(
            color=alt.value('red'),
            opacity=alt.value(0.7),
            x=alt.X(
                'year:O', 
                scale=alt.Scale(domain=list(all_years)),
                title='Year'
            ),
            y=alt.Y(
                'sea_ice_extent',
                title='SIE (km²)'       
            )
        )
        .properties(
            title='April 17th Artic Sea Ice Extent'
        )
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
