# CS 6240 Fall 2017
# Makefile for Assignment A7
# Author: Shreysa Sharma
# Date: 31 October 2017


# === Modify this to reflect local installation === #
SCALA_HOME = $(HOME)/tools/scala-2.11.11
SPARK_HOME = $(HOME)/tools/spark-2.2.0-bin-hadoop2.7
INPUT_SONGS_FILE_PATH=./data/small/song_info.csv
INPUT_ARTIST_TERMS_FILE_PATH=./data/small/artist_terms.csv

# === Verify this path === #
SPARK_CLASS_PATH = "$(SPARK_HOME)/jars/*"

# === DO NOT CHANGE ANYTHING BELOW THIS LINE ===# 
TARGET_FOLDER = target
BINARY_NAME = a7.jar
SCALA = $(SCALA_HOME)/bin/scala
SCALAC = $(SCALA_HOME)/bin/scalac
SPARK_SUBMIT = $(SPARK_HOME)/bin/spark-submit
CLASSPATH = $(SPARK_CLASS_PATH):$(TARGET_FOLDER)/$(BINARY_NAME)

SRC_FOLDER = src
SOURCES = $(shell find src -name "*.scala" -type f)

default: all

init:
	@$(RM) -rf $(TARGET_FOLDER)
	@mkdir $(TARGET_FOLDER)

all: clean build run report

build: init
	@$(RM) -rf output
	@$(SCALAC) -classpath $(SPARK_CLASS_PATH) -d $(TARGET_FOLDER) src/*.scala
	@echo "Compressing to jar: " $(TARGET_FOLDER)/$(BINARY_NAME)
	@jar -cmf $(SRC_FOLDER)/MANIFEST.MF $(TARGET_FOLDER)/$(BINARY_NAME) -C $(TARGET_FOLDER) . 

report:
	@echo "Generating report..."
	Rscript -e 'library(rmarkdown); rmarkdown::render("./report.Rmd", "html_document", "pdf_document")' 

run:
	@$(SPARK_SUBMIT) --master local[8] $(TARGET_FOLDER)/$(BINARY_NAME) $(INPUT_SONGS_FILE_PATH) $(INPUT_ARTIST_TERMS_FILE_PATH)

classpath:
	@echo "Classpath: \n" $(CLASSPATH)

%.class: %.scala
	@echo "Building $*.scala"
	#@$(SCALAC) -classpath $(SPARK_CLASS_PATH) -d $(TARGET_FOLDER) $*.scala
	$(SCALAC) -classpath $(SPARK_CLASS_PATH) -d $(TARGET_FOLDER) src/*.scala

clean:
	@$(RM) -rf $(TARGET_FOLDER)
	@$(RM) -rf spark-warehouse
	@$(RM) -rf metastore_db
	@$(RM) -rf project
	@$(RM) derby.log
