

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
                (pl.col('value') * 9/5 + 32).alias('temp_f')
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
    altered = clean_source(df, 'FLs.52j')
    return (altered,)


@app.cell
def _(altered, pl, raw):
    combined = (
        altered
        .join(
            raw,
            on=['coop_id', 'year', 'month'],
            how='left',
            suffix='_raw'
        )
        .sort(['year', 'month', 'coop_id'])
        .select(['year', 'month', 'coop_id', 'temp_f', 'dmflag', 'temp_f_raw', 'dmflag_raw'])
        .with_columns(
            (pl.col('temp_f') - pl.col('temp_f_raw')).alias('adjustment')
        )
    )
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
            # Number of altered temp. observations
            pl.col('temp_f').is_not_null().sum().alias('n_obs_altered'),
            # Number of raw temp. observations
            pl.col('temp_f_raw').is_not_null().sum().alias('n_obs_raw'),
            # Number of dropped raw temp. observations
            pl.col('temp_f_raw').filter(pl.col('temp_f').is_null()).count().alias('n_dropped_obs'),
            # Number of temp. observations that exist is both raw and altered data
            pl.col('adjustment').count().alias('n_obs_both'),
            # Number of temp. observations that exist is both raw and altered data that have been altered
            pl.col('adjustment').filter(pl.col('adjustment') != 0).count().alias('n_adjusted_obs'),
            # Number of temp. observations that have been estimated
            pl.col('temp_f').filter((pl.col('dmflag') == 'E') & (pl.col('temp_f').is_not_null())).count().alias('n_estimated_obs'),
            # Average adjusment
            pl.col('adjustment').filter(pl.col('adjustment') != 0).mean().alias('avg_adjustment'),
            # Average temp. of obs. that were estimated
            pl.col('temp_f').filter((pl.col('dmflag') == 'E') & (pl.col('temp_f').is_not_null())).mean().alias('avg_estimate'),
            # Average raw temp. of obs. that were not estimated
            pl.col('temp_f_raw').filter((pl.col('dmflag') != 'E') | pl.col('dmflag').is_null()).mean().alias('avg_non_estimated_raw'),
        )
        .with_columns(
            (100 * pl.col('n_adjusted_obs') / pl.col('n_obs_raw')).alias('percent_adjusted'),
            (100 * (pl.col('n_adjusted_obs') + pl.col('n_dropped_obs'))/ pl.col('n_obs_raw')).alias('percent_adjusted_or_dropped'),
            (pl.col('avg_estimate') - pl.col('avg_non_estimated_raw')).alias('avg_estimate_delta'),
        )
    )
    return (stats,)


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
    make_chart(stats, 'percent_adjusted_or_dropped', 'n_obs_raw', "When raw data already existed, here's how frequently it was adjusted or dropped")
    return


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
    make_chart(stats, 'avg_estimate_delta', 'n_estimated_obs', "When raw data did not exist, here's how it was estimated\n(relative to the average of existing raw data)")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
