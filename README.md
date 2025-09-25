# Analysis of Electricity Sources in the United States

The Energy Information Administration (EIA) publishes a wealth of energy-related data collected across the country. In this project, I analyzed the sources of electricity generated in the United States. I used Apache Airflow to implement an automated ELT pipeline that extracts power plant electricity generation data from EIA’s API, loading it into a Google Cloud Service (GCS) bucket and subsequently into a BigQuery dataset. Also included in this project's GCS/BigQuery server is a data set of power plant locations (used their coordinates to create my dashboard's map visual), which I downloaded as a flat file, processing it to only include the columns I was interested in, from EIA’s website. After loading everything into BigQuery, the data was transformed with SQL queries to create the data model, and the associated data tables were loaded into Power BI for visual analysis.

To avoid posting any keys and credentials, I only uploaded the Python script for my Airflow DAG, SQL queries, and the docker-compose yaml file.

My interactive Power BI report can be viewed here (updated Aug. 17, 2025): https://app.powerbi.com/view?r=eyJrIjoiNmY2MjFhMTctNTVjZi00MjZmLWI1MjktMjEwYjdiMWU3YzA2IiwidCI6ImJlMjI4MDdiLTI1OTEtNDBkNy1iYmI2LTBkYTg0ZDMxYWNmNyIsImMiOjN9

EIA regularly updates their data retroactively, which is why there are noticeable decreasing trends in recent months, and smaller power plants generally take longer to get updated. Furthermore, upon data collection, the most recent available data is from a few months prior.

The report's pbix file can be downloaded here: https://drive.google.com/file/d/1Y1nv5ilco_Kel8PtT-yNfJnwBED6vfgn

Below is an image of my report:

<p align="center">
<img src="images/dashboard.jpg" alt="Alt text" width="1000"/>
</p>

## ELT Pipeline and Data Model

- **api_gcs_bq_usa_dag.py:** This Python script creates the Airflow DAG, pictured below.
  - <p align="center">
    <img src="images/dag.jpg" alt="Alt text" width="1000"/>
    </p>
  -	The DAG automates the pipeline as such:
    -	(1) latest_only ensures only the latest run is executed when the pipeline is initiated.
    -	(2) Download power plant electricity data via API calls to Docker container.
    -	(3) Upload the data from container to GCS.
    -	(4) Load data from GCS to BigQuery for SQL processing.
    -	(5) Load the power plants location data, which was uploaded manually to the GCS bucket, from GCS to BigQuery.
    -	(6) Run the dates.sql queries to create dates table for PBI
    -	(7) Run the process_df_usa.sql queries to process raw data fetched from the API calls. This file selects desired columns, recategorizes energy sources into broader categories (e.g., Wood Waste Solids is recategorized as Other Renewables), categorize states into regions, perform unit conversion, removes rows that would double-count generation data, and corrects geogrpahic errors in the data
    -	(8) Run the update_power_plants.sql queries to correct geographic errors in the power plants' location data.
  -	The final data tables, called **usa_processed**, **dates**, and **power_plants**, are imported from BigQuery to Power BI. As of Feb. 10, 2025, **usa_processed** contains 6,165,628 rows of data.
  -	Since I use the free tier of Power BI Service, I am unable to implement an auto-refresh feature that updates the dashboard automatically with new BigQuery data. I would have to re-import the updated data in Power BI after the scheduled DAG is executed, and publish it to Service manually. I will make attempts to do this and update this repo each month.

Below is an image of the data model in Power BI, demonstrating the relationships between the imported tables for this PBI report.

<p align="center">
<img src="images/data-model.jpg" alt="Alt text" width="1000"/>
</p>

## Summary of Findings

Note 1: Some of findings/visuals here are associated with an older version of the report (i.e., date range ends sooner than in published report).
Note 2: Total generation here means total net generation, that is, gross generation minus electricity consumed by power plants.

Across the entire data set, coal, natural gas, nuclear, and renewables have been the top sources of electricity. Electricity generated from coal has been declining since 2008, while the share of generation from natural gas and renewable energy sources has been steadily increasing. Electricity generated from nuclear power has been steadily consistent. In 2024 (through Sep.), natural gas was the largest source of electricity, more than double the next largest source, nuclear, which produced similar amounts of electricity as coal and renewables. 

<p align="center">
<img src="images/usa-all.jpg" alt="Alt text" width="600"/>
</p>

Among renewable energy sources (across the entire data set), hydro is the dominant source of electricity, followed by wind. In recent years, however, wind has taken the top spot, while hydro and solar have converged. 

<p align="center">
<img src="images/usa-renewables.jpg" alt="Alt text" width="600"/>
</p>

Constellation Nuclear generated the most electricity across the country in Q3 of 2024 with over 45 TWh.

The date range and other filters can be modified for further analysis.

<p align="center">
<img src="images/top-utility-usa.jpg" alt="Alt text" width="1000"/>
</p>

Below, I summarize my findings for each region. More detailed analysis can be performed with the interactive PBI report.

### Midwest

The Midwest somewhat mirrors national trends, except in recent years, the amount of electricity generated by the top four sources (coal, nuclear, natural gas, and renewables) have converged, with renewables appearing to dip.

<p align="center">
<img src="images/midwest-all.jpg" alt="Alt text" width="600"/>
</p>

Among renewables, wind is the dominant source of electricity, and it has been growing steadily as a source since 2007. The rest have been relatively low and stable in their output, but hydro has decreased in recent years, while solar has been seeing a slight increase.

<p align="center">
<img src="images/midwest-renewables.jpg" alt="Alt text" width="600"/>
</p>

Constellation Nuclear's power plants were the largest producers of electricity in the Midwest in Q3 of 2024 (almost 25 TWh). Similar to nationwide trend, its Midwest trend is also relatively consistent in output from nuclear energy.

### Northeast

As a source of electricity, coal has been decreasing since 2007, and in 2024, it made up only 2.3% of all electricity generated. Natural gas is the largest source of electricity in 2024, taking 57% of the share.

<p align="center">
<img src="images/northeast-all.jpg" alt="Alt text" width="600"/>
</p>

Hydro constitutes the bulk of electricity generated from renewable sources.

<p align="center">
<img src="images/northeast-renewables.jpg" alt="Alt text" width="600"/>
</p>

Again, Constellation Nuclear was the largest producer of electricity in the Northeast in Q3 of 2024 at over 16 TWh. Again, the trend over time is pretty consistent.

### South

Across the entire data set, the South generated by far the most electricity (~43,000 TWh), almost doubling the next closest region (~22,000 TWh from the Midwest). These values can be viewed in the dashboard’s matrix visual. Overall trends closely mirror national trends.

<p align="center">
<img src="images/south-all.jpg" alt="Alt text" width="600"/>
</p>

Wind has been the largest renewable source, but solar is catching up and overtook hydro in 2022.

<p align="center">
<img src="images/south-renewables.jpg" alt="Alt text" width="600"/>
</p>

The most productive utility in the South in Q3 of 2024 was Florida Power & Light Co at 41 TWh, with most of their electricity coming from natural gas.

### West

In the West, renewables and natural gas are the top two generators of electricity, outputting similar amounts of energy. These two sources generally peak and dip at off cycles from each other.

<p align="center">
<img src="images/west-all.jpg" alt="Alt text" width="600"/>
</p>

For most of the data set, hydro was the largest renewable source of electricity, but in recent years, it has been trending gradually downward, while wind and solar have been increasing. Latest trends appear to have these three sources converging soon.

<p align="center">
<img src="images/west-renewables.jpg" alt="Alt text" width="600"/>
</p>

Arizona Public Service Co. generated the most electricity in Q3 of 2024 with 14 TWh.

<p align="center">
<img src="images/top-utility-west.jpg" alt="Alt text" width="1000"/>
</p>



