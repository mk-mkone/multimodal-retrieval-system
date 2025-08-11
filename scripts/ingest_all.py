from app.ingestion.simulation_ingestor import MaterialsProjectIngestor
from app.ingestion.text_ingestor import EuropePMCIngestor
from app.ingestion.timeseries_ingestor import TimeSeriesIngestor


def main() -> None:
    text = EuropePMCIngestor()
    df_text = text.run(query="materials informatics", page=1, page_size=10)
    print(f"[text] rows: {len(df_text)}")

    sim = MaterialsProjectIngestor()
    df_sim = sim.run(formula="Si", per_page=10)
    print(f"[simulation] rows: {len(df_sim)}")

    ts = TimeSeriesIngestor()
    df_ts = ts.run(path="data/raw/example_timeseries.csv")
    print(f"[timeseries] rows: {len(df_ts)}")


if __name__ == "__main__":
    main()
