# README for open-FF repository and project

This CodeOcean capsule is system of code to transform data from 
the online chemical disclosure site 
for hydraulic fracturing, FracFocus.org, into a usable database.  

The code performs cleaning, flagging, and 
curating techniques to yield organized data sets and sample analyses 
from a difficult collection of chemical records.   
For a majority of the chemical records, the mass of the chemicals used 
in fracking operations is calculated. 

The output of this project includes full data sets and filtered data sets. All 
sets include many of the original raw FracFocus fields and many generated
fields that correct and add context to the raw data.  The full sets do not 
filter out any of the original raw FracFocus records but leaves that up to 
the user (by using the record_flags field, etc.)  Filtered data sets remove
the FracFocus records that have significant problems to give the user a 
product that is usable without much work.

To be included in filtered data sets, 
- Fracking events must use water as carrier and percentages must be 
    consistent and within tolerance.
- Chemicals must be identified by a match with an authoritative CAS number 
    or be labeled proprietary.

Further, portions of the raw bulk data that are filtered out include: 
- fracking events with no chemical records (mostly 2011-May 2013; but are 
    replaced with the SkyTruth archive).
- fracking events with multiple entries (and no indication which entries 
    are correct).
- chemical records that are identified as redundant within the event.

Finally,  I clean up some of the labeling fields by consolidating multiple 
versions of a single category into an easily searchable name. For instance, 
I collapse the 80+ versions of the supplier name 'Halliburton' to a single
value 'halliburton'.

This code is designed to facilitate adding new disclosures to data sets 
periodically (after curation) to keep the output data sets relatively 
up to date.

By removing or cleaning the difficult data from this unique data source, 
I have produced a data set that should facilitate more in-depth 
analyses of chemical use in the fracking industry.


## Versions of Open-FF 

**Version 10**:
- Text indications of missing values in CASNumber, IngredientName, Supplier
    consolidated into the single token: "MISSING." See the tranlation table,
    \data\missing_values.csv.
- CAS numbers are now curated completely by hand using both CASNumber and IngredientName. This
    change allows for many simple typos to be corrected and for many obviously
    wrong CASNumbers to be changed or flagged as 'ambiguous'.  Further, this curation
    produces a more thorough characterization of propritary claims and the
    'category' of the change is available to the end user for further analysis.
    The translation file between original CASNumber/IngredientName pairs and
    resulting bgCAS is in the /sources directory ("/data" folder for CodeOcean).
    Because the CASNumber of the water carrier was sometimes mislabeled, this
    change corrects those problems and consequently, opens many more disclosures
    to mass calculations for all chemicals.
- Records that are likely fracking job 'carriers' (that is, percent of the fracking
    fluid is greater that 50%) but where the CAS identification is missing are
    now curated by hand.  Currently this is about 30,000 disclosures and the
    vast majority of them are obviously water-based carriers.  This evaluation
    allows us to calculate the mass of the chemicals in the disclosures. This
    change has added about 800,000 mass records to the data set. If the user
    wishes to continue to filter out these disclosures for mass (as was the case
    for pervious Open-FF versions), a simple flag makes that task simple.
- The code and reference files created to translate SciFinder CAS naming is
    now included in the core file "process_CAS_ref_files.py."
- The code used to do pre-processing can be found in the builder_tasks folder, 
    though not necessarily performed in CodeOcean. It is included for completeness
    and transparency.  The output of these scripts are mostly held in the 
    /data (/sources) folder.
- The calculation of mass for every chemical now incorporates the density of the
    carrier fluid indicated in the IngredientComments field, when available. This is
    available for about XXX% of disclosures.
- Large-scale refactoring of code to removed unused or overly complicated 
    sections.
- ClusterID changed to fixed length string for more consistent searches.
- Individual boolean flags available for fine-grained filtering.
- Classes of code to create data sets in a consistent manner across different
    bulk data inputs in the Analysis_set class and subclasses.
- Include a more comprehensive and searchable Data Dictionary with tables
    showing components of canned data sets.

**Version 9**: 
- Data download from FracFocus on March 5, 2021.
- Correct FF_stats.py calculation for percent non-zero in the integer
    and float section. 
- Generate geographic clusters as proxy of wellpad identity;
    clusters are found in the string field "clusterID". (Note that a specific clusterID will NOT be
    consistent across data set versions in the way that UploadKey is; don't depend
    on it!).  
- The fields FederalWell and IndianWell have been changed to string type -
    previously, they were boolean (T/F) but that type does not allow for empty
    cells which occurs in the SkyTruth data, leading to misinformation. 
- Added PercentHighAdditive to full data output to allow for better investigations
    of TradeName usage. 
- Rename the old field 'infServiceCo' to 'primarySupplier' to
    better reflect its generation.
- Added chemical lists of the Clean Water Act, the Safe Drinking Water Act,
    and from California's Proposition 65 lists to help identify chemicals of
    concern.

**Version 8.1**: Correct slight documentation omission.

**Version 8**: Added WellName field to filtered data output.  Added chemical ingredient
   codes from the WellExplorer project (www.WellExplorer.org) -- fields with
   the prefix 'we_' are from that project. See https://doi.org/10.1093/database/baaa053
   Data download from FracFocus on October 23, 2020.

**Version 7**: Data downloaded from FracFocus on July 31, 2020.  TradeName added
   to exported data.

**Version 6**: Save data tables in pickled form into results section so that it may be
   exported to other projects.

**Version 5**: Data downloaded from FracFocus on May 14, 2020.  No other changes.

**Version 4**: Data downloaded from FracFocus on March 20, 2020.  Added the generated
   field, infServiceCo. This field is an attempt to identify the primary
   service company of a fracking event.  Including in the output files the 
   raw field 'Projection' which is needed to accurately map using lat/lon
   data.

**Version 3**: Data downloaded from FracFocus on Jan. 22, 2020. Modified the 
   FF_stats module to generate separate reports for the "bulk download" and
   the "SkyTruth" data sources.  Both are reported in "ff_raw_stats.txt" in the
   results section.)

**Version 2**: Data downloaded from FracFocus on Jan. 22, 2020. Incorporated 
   basic statistics on the raw FracFocus data (see "ff_raw_stats.txt" in the
   results section.)

**Version 1**: Data downloaded from FracFocus on Jan. 22, 2020. Similar 
   to the Proof-of-Concept version with the following new features:
   SkyTruth archive
   has been incorporated.  Links to references include: Elsner & Hoelzer 2016, 
   TEDX chemical list and TSCA list.


