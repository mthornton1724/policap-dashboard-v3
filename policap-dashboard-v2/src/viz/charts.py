import matplotlib.pyplot as plt

def line_chart(df, x_col, y_cols, title):
    fig, ax = plt.subplots()
    for col in y_cols:
        if col in df.columns:
            ax.plot(df[x_col], df[col], label=col)
    ax.set_title(title)
    ax.set_xlabel(x_col)
    ax.set_ylabel("value")
    ax.legend()
    return fig
