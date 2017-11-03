## CS6240 - Fall 2017
### Assignment A7
### Shreysa Sharma and Jashangeet Singh

### How to read
The centroids for each cluster for loudness, duration, tempo and hotness are printed to console.
The combined hotness details are sent to output folder
The agglomerative clustering prints output to console (However the centroids are not being calculated)

## Running 
- Clone the repository
- Update `SCALA_HOME` and `SPARK_HOME` in the `Makefile`
- create a folder data and place the input files
- Update variable INPUT_SONGS_FILE_PATH=the path of song_info.csv in your system
- Update variable INPUT_ARTIST_TERMS_FILE_PATH= the path of artist_terms.csv in your system
- Verify `SPARK_CLASS_PATH` (For some installations the lib path is `libexec/jars/*`)
- `make` this will clean, build, run the program and generate the report

## Requirements
- Scala - used version `2.11.11`. *NOTE* using `2.12.4` results in error (`java.lang.NoClassDefFoundError: scala/runtime/LambdaDeserialize`)
- Apache Spark - used version `spark-2.2.0-bin-hadoop2.7`

- driver memory and executor memory have been set to 2 GB on the local system on which the program was run, one can run with any configuration but this could affect the time taken to get the desired output
