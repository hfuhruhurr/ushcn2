

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


@app.cell
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


@app.cell
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
        .select(['year', 'month', 'coop_id', 'temp', 'dmflag', 'temp_raw', 'dmflag_raw', 'temp_tob', 'dmflag_tob'])
        .with_columns(
            (pl.col('temp_tob') - pl.col('temp_raw')).alias('adjustment_tob'),
            (pl.col('temp') - pl.col('temp_tob')).alias('adjustment_pha'),
            (pl.col('temp') - pl.col('temp_raw')).alias('adjustment'),
        )
    )
    combined
    return (combined,)


@app.cell
def _(combined, pl):
    stats = (
        combined
        # .filter(
        #     pl.col('month') == 7
        # )
        .group_by('year')
        .agg(
            # Number of observations
            pl.col('temp').is_not_null().sum().alias('n_obs_pha'),
            pl.col('temp_tob').is_not_null().sum().alias('n_obs_tob'),
            pl.col('temp_raw').is_not_null().sum().alias('n_obs_raw'),
        
            # Number of dropped observations
            pl.col('temp_raw').filter(pl.col('temp_tob').is_null()).count().alias('n_tob_dropped_obs'),
            pl.col('temp_tob').filter(pl.col('temp').is_null()).count().alias('n_pha_dropped_obs'),
        
            # # Number of temp. observations that exist is both raw and altered data
            # pl.col('adjustment_tob').count().alias('n_obs_fboth'),
        
            # # Number of temp. observations that exist is both raw and altered data that have been altered
            # pl.col('adjustment').filter(pl.col('adjustment') != 0).count().alias('n_adjusted_obs'),
        
            # Number of temp. observations that have been estimated
            pl.col('temp').filter(
                (pl.col('dmflag') == 'E') & (pl.col('temp').is_not_null())
            ).count().alias('n_obs_estimated'),

            # Number of obs that have been adjusted
            pl.col('temp_tob').filter(pl.col('adjustment_tob') != 0).count().alias('n_tob_adjusted'),
            pl.col('temp').filter(pl.col('adjustment_pha') != 0).count().alias('n_pha_adjusted'),
            pl.col('temp').filter(pl.col('adjustment') != 0).count().alias('n_adjusted'),
        
            # Average adjusment
            pl.col('adjustment_tob').filter(pl.col('adjustment_tob') != 0).mean().alias('avg_tob_adjustment'),
            pl.col('adjustment_pha').filter(pl.col('adjustment_pha') != 0).mean().alias('avg_pha_adjustment'),
            pl.col('adjustment').filter(pl.col('adjustment') != 0).mean().alias('avg_adjustment'),
        
            # Average temp. of obs. that were estimated
            pl.col('temp').filter(pl.col('dmflag') == 'E').mean().alias('avg_estimate'),
        
            # Average raw temp. of obs. that were not estimated
            pl.col('temp_raw').filter(
                (pl.col('dmflag') != 'E') 
                | pl.col('dmflag').is_null()
            ).mean().alias('avg_non_estimated_raw'),
        )
        .with_columns(
            (100 * pl.col('n_tob_adjusted') / pl.col('n_obs_raw')).alias('percent_tob_adjusted'),
            (100 * pl.col('n_pha_adjusted') / pl.col('n_obs_tob')).alias('percent_pha_adjusted'),
            (100 * pl.col('n_adjusted') / pl.col('n_obs_raw')).alias('percent_adjusted'),
            (pl.col('avg_estimate') - pl.col('avg_non_estimated_raw')).alias('avg_estimate_delta'),
        )
    )
    return (stats,)


@app.cell
def _(stats):
    stats
    return


@app.cell
def _(pl, plt):
    def make_chart(df: pl.DataFrame, line_field: str , bar_field: str, title: str):
        left_y_axis_color = 'blue'
        right_y_axis_color = 'green'

        if line_field.startswith('percent'):
            y_min = 0
        else:
            y_min = None

        # Create the figure and axis
        fig, ax = plt.subplots(figsize=(9, 5))

        line = ax.plot(df['year'], df[line_field], color=left_y_axis_color, linewidth=2, label=line_field)[0]

        # Set labels and title for the left y-axis
        plt.xlabel('Year')
        plt.ylabel(line_field, color=left_y_axis_color)
        plt.tick_params(axis='y', labelcolor=left_y_axis_color)
        ax.set_ylim(y_min, None)  # Set left y-axis minimum to 0
        plt.title(title)
        plt.grid(True)

        # Create a second y-axis for the bar plot
        ax2 = plt.twinx()
        # Plot bars 
        bars = ax2.bar(df['year'], df[bar_field], color=right_y_axis_color, alpha=0.5, label=bar_field, width=0.4)

        # Set labels for the right y-axis
        ax2.set_ylabel(bar_field, color=right_y_axis_color)
        ax2.tick_params(axis='y', labelcolor=right_y_axis_color)

        # Combine legends from both axes
        plt.legend([line, bars], [line_field, bar_field], loc='upper left')

        # Adjust layout
        plt.tight_layout()

        return fig
    return (make_chart,)


@app.cell
def _(make_chart, stats):
    make_chart(stats, 'percent_adjusted', 'n_obs_raw', "When raw data already existed, here's how frequently it was adjusted")
    return


@app.cell
def _(make_chart, stats):
    make_chart(stats, 'avg_adjustment', 'n_obs_raw', "When raw data already existed, here's how it was adjusted")
    return


@app.cell
def _(make_chart, stats):
    make_chart(stats, 'avg_tob_adjustment', 'n_obs_raw', "When raw data already existed, here's how it was adjusted for TOD Bias")
    return


@app.cell
def _(make_chart, stats):
    make_chart(stats, 'avg_estimate_delta', 'n_obs_estimated', "When raw data did not exist, here's how it was estimated\n(relative to the average of existing raw data)")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
