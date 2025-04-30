

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import matplotlib.pyplot as plt
    return pl, plt


@app.cell
def _(pl):
    df = pl.read_parquet('source-data/ushcn/ushcn_monthly_data.parquet')
    return (df,)


@app.cell(hide_code=True)
def _(pl):
    def clean_source(df, dataset_type):
        return (
            df
            .filter(
                pl.col('element') == 'tmax',
                pl.col('dataset_type') == dataset_type,
            )
            .with_columns(
                pl.col('value').alias('temp_c'),
                (pl.col('value') * 9/5 + 32).alias('temp')
            )
            .drop(['element', 'dataset_type', 'value'])
        )
    return (clean_source,)


@app.cell
def _(clean_source, df):
    raw = clean_source(df, 'raw')
    return (raw,)


@app.cell
def _(clean_source, df):
    tob = clean_source(df, 'tob')
    return (tob,)


@app.cell
def _(clean_source, df):
    pha = clean_source(df, 'FLs.52j')
    return (pha,)


@app.cell(hide_code=True)
def _(pha, pl, raw, tob):
    combined = (
        pha
        .join(
            raw,
            on=['coop_id', 'year', 'month'],
            how='left',
            suffix='_raw'
        )
        .join(
            tob,
            on=['coop_id', 'year', 'month'],
            how='left',
            suffix='_tob'
        )
        .sort(['year', 'month', 'coop_id'])
        .rename({'temp': 'temp_pha', 'dmflag': 'dmflag_pha'})
        .select(['year', 'month', 'coop_id', 'temp_raw', 'temp_tob', 'temp_pha', 'dmflag_raw', 'dmflag_tob', 'dmflag_pha'])
        .with_columns(
            # Determine the adjustment
            (pl.col('temp_tob') - pl.col('temp_raw')).alias('tob_adjustment'),
            (pl.col('temp_pha') - pl.col('temp_tob')).alias('pha_adjustment'),
            # Add dropped flags
            (pl.col('temp_tob').is_null() & pl.col('temp_raw').is_not_null()).alias('tob_dropped'),
            (pl.col('temp_pha').is_null() & pl.col('temp_tob').is_not_null()).alias('pha_dropped'),
        )
        .with_columns(
            # Add adjusted flags
            (pl.col('tob_adjustment') != 0).alias('tob_adjusted'),
            (pl.col('pha_adjustment') != 0).alias('pha_adjusted'),
        )
    )
    return (combined,)


@app.cell(hide_code=True)
def _(combined, pl):
    stats = (
        combined
        .group_by('year')
        .agg(
            # Number of observations
            pl.col('temp_raw').count().alias('n_raw'),
            pl.col('temp_tob').count().alias('n_tob'),
            pl.col('temp_pha').count().alias('n_pha'),

            # Number of dropped observations
            pl.col('tob_dropped').sum().alias('n_tob_dropped'),
            pl.col('pha_dropped').sum().alias('n_pha_dropped'),

            # Number of obs that have been adjusted
            pl.col('tob_adjusted').sum().alias('n_tob_adjusted'),
            pl.col('pha_adjusted').sum().alias('n_pha_adjusted'),
                
            # Average adjusment
            pl.col('tob_adjustment').mean().alias('avg_tob_adjustment'),
            pl.col('pha_adjustment').mean().alias('avg_pha_adjustment'),
        
            # Number of temp. observations that have been estimated
            pl.col('temp_pha').filter((pl.col('dmflag_pha') == 'E')).count().alias('n_estimated'),
        
            # Average temp. of obs. that were estimated
            pl.col('temp_pha').filter(pl.col('dmflag_pha') == 'E').mean().alias('avg_estimated_temp'),

            # Average raw temp. of obs. that were not estimated
            pl.col('temp_raw').filter(
                (pl.col('dmflag_pha') != 'E') 
                | pl.col('dmflag_pha').is_null()
            ).mean().alias('avg_non_estimated_raw_temp'),
        )
        .with_columns(
            (100 * pl.col('n_tob_adjusted') / pl.col('n_raw')).alias('percent_tob_adjusted'),
            (100 * pl.col('n_pha_adjusted') / pl.col('n_tob')).alias('percent_pha_adjusted'),
            (pl.col('avg_estimated_temp') - pl.col('avg_non_estimated_raw_temp')).alias('avg_estimate_delta'),
        )
    )
    return (stats,)


@app.cell(hide_code=True)
def _(pl, plt):
    def make_chart(df: pl.DataFrame, line_field_1: str, line_field_2: str , bar_field: str, title: str):
        left_y_axis_color = 'blue'
        right_y_axis_color = 'green'

        if line_field_1.startswith('percent'):
            y_min = 0
            y1_label = '% of Observations Adjusted'
        else:
            y_min = None
            y1_label = 'Temp. Delta (F)'

        if bar_field == 'n_raw':
            y2_label = '# of Raw Observations'
        elif bar_field == 'n_estimated':
            y2_label = '# of Estimated Observations'
        else:
            y2_label = bar_field
        
        # Create the figure and axis
        fig, ax = plt.subplots(figsize=(9, 5))

        line_1 = ax.plot(df['year'], df[line_field_1], color=left_y_axis_color, linewidth=1, label=line_field_1)[0]
        if line_field_2:
            line_2 = ax.plot(df['year'], df[line_field_2], color=left_y_axis_color, linewidth=1, label=line_field_2, linestyle='--')[0]
    
        # Set labels and title for the left y-axis
        plt.xlabel('Year')
        plt.ylabel(y1_label, color=left_y_axis_color)
        plt.tick_params(axis='y', labelcolor=left_y_axis_color)
        ax.set_ylim(y_min, None)  # Set left y-axis minimum to 0
        plt.title(title)
        plt.grid(True)

        # Create a second y-axis for the bar plot
        ax2 = plt.twinx()
    
        # Plot bars 
        bars = ax2.bar(df['year'], df[bar_field], color=right_y_axis_color, alpha=0.5, label=bar_field, width=0.4)

        # Set labels for the right y-axis
        ax2.set_ylabel(y2_label, color=right_y_axis_color)
        ax2.tick_params(axis='y', labelcolor=right_y_axis_color)

        # Combine legends from both axes
        if line_field_2:
            plt.legend([line_1, line_2, bars], [line_field_1, line_field_2, bar_field], loc='upper left')
        else:
            plt.legend([line_1, bars], [line_field_1, bar_field], loc='upper left')

        # Adjust layout
        plt.tight_layout()

        return fig
    return (make_chart,)


@app.cell
def _(make_chart, stats):
    make_chart(stats, 'percent_tob_adjusted', 'percent_pha_adjusted', 'n_raw', "When raw data already existed, here's how frequently it was adjusted")
    return


@app.cell
def _(make_chart, stats):
    make_chart(stats, 'avg_tob_adjustment', 'avg_pha_adjustment', 'n_raw', "When raw data already existed, here's how it was adjusted")
    return


@app.cell
def _(make_chart, stats):
    make_chart(stats, 'avg_estimate_delta', '', 'n_estimated', "When raw data did not exist, here's how it was estimated\n(relative to the average of existing raw data)")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
