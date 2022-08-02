# ecf-dedup

Script to de-duplicate, sanitize, and spatially join
[ECF](https://opendata.usac.org/Emergency-Connectivity-Fund/Emergency-Connectivity-Fund-FCC-Form-471/i5j4-3rvr)
(Emergency Connectivity Fund) data.

This is used in conjunction with the USA ECF Tableau dashboard, located at:
[go.ncsu.edu/2022ecfdashboard-usa](https://go.ncsu.edu/2022ecfdashboard-usa)

## Quickstart

This project requires Python 3.10 and [Poetry](https://python-poetry.org/).

For recreating the USA dashboard's data source, three files are required:

1. ECF Data for the USA (or optionally your chosen state; any ECF data pulled from
   [opendata.usac.org](https://opendata.usac.org/) will work)
    - If no ECF filepath is provided, it will automatically pull the latest version from
      [here](<(https://opendata.usac.org/Emergency-Connectivity-Fund/Emergency-Connectivity-Fund-FCC-Form-471/i5j4-3rvr)>).
2. E-rate supplemental entity information,
   [link](https://opendata.usac.org/E-rate/E-Rate-Supplemental-Entity-Information/7i5i-83qf).
   _warning, the file is rather large at ~110Mb_.
3. Shapefile of the school districts of the USA,
   [link](https://nces.ed.gov/programs/edge/Geographic/DistrictBoundaries). _warning,
   the file is rather large at ~180Mb_.

After retrieving the above, run the script.

### Building the dashboard

In order for Tableau to interpret the shapefile properly, the geometry from the
resultant GeoDataFrame is dropped. Instead we join on a row's unique key, the `GEOID`
(more information can be found in the School District Documentation
[file](https://nces.ed.gov/programs/edge/docs/EDGE_SDBOUNDARIES_COMPOSITE_FILEDOC.pdf)).
