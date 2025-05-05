import os
import glob
import ijson


# generator for iterating over JSON reports

def iterate_reports_ijson(path):
    """Yields one report at a time from the 'results' array inside the full dataset,
    iterating over all .json files if a directory is provided."""
    
    def yield_file(file_path):
        with open(file_path, 'rb') as f:
            parser = ijson.items(f, 'results.item')
            for report in parser:
                yield report

    # If path is a directory, iterate over all JSON files inside
    if os.path.isdir(path):
        for file_path in sorted(glob.glob(os.path.join(path, '*.json'))):
            yield from yield_file(file_path)
    else:
        yield from yield_file(path)