from utils import split_date_range
from datetime import datetime

def search_with_date_split(search_func, query, start_date, end_date, **kwargs):
    """
    Orchestrates one or multiple searches, splitting by date window for API result limit compliance.
    search_func should be a function (like search_single_query) accepting start_date and end_date as kwarg strings.
    """
    all_csv_paths = []
    dt_start = datetime.strptime(start_date, "%d/%m/%Y")
    dt_end   = datetime.strptime(end_date, "%d/%m/%Y")
    date_chunks = split_date_range(dt_start, dt_end, days=kwargs.get('days_per_chunk',7))
    for idx, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
        csv_path = search_func(
            query=query,
            start_date=chunk_start.strftime("%d/%m/%Y"),
            end_date=chunk_end.strftime("%d/%m/%Y"),
            **{k:v for k,v in kwargs.items() if k not in ['days_per_chunk']}
        )
        if csv_path: all_csv_paths.append(csv_path)
    return all_csv_paths
