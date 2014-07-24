package io.prediction.engines.java.recommendations;

import io.prediction.controller.java.LJavaDataSource;
import io.prediction.controller.EmptyParams;
import scala.Tuple2;
import scala.Tuple3;
import java.io.File;
import java.io.FileNotFoundException;
import java.lang.Iterable;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSource extends LJavaDataSource<
  DataSourceParams, EmptyParams, TrainingData, Query, EmptyData> {

  final static Logger logger = LoggerFactory.getLogger(DataSource.class);

  DataSourceParams params;

  public DataSource(DataSourceParams params) {
    this.params = params;
  }

  @Override
  public Iterable<Tuple3<EmptyParams, TrainingData, Iterable<Tuple2<Query, EmptyData>>>> read() {

    File ratingFile = new File(params.filePath);
    Scanner sc = null;

    try {
      sc = new Scanner(ratingFile);
    } catch (FileNotFoundException e) {
      logger.error("Caught FileNotFoundException " + e.getMessage());
      System.exit(1);
    }

    List<TrainingData.Rating> ratings = new ArrayList<TrainingData.Rating>();

    while (sc.hasNext()) {
      String line = sc.nextLine();
      String[] tokens = line.split("[\t,]");
      try {
        TrainingData.Rating rating = new TrainingData.Rating(
          Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]),
          Float.parseFloat(tokens[2]));
        ratings.add(rating);
      } catch (Exception e) {
        logger.error("Can't parse rating file. Caught Exception: " + e.getMessage());
        System.exit(1);
      }
    }

    List<Tuple3<EmptyParams, TrainingData, Iterable<Tuple2<Query, EmptyData>>>> data =
      new ArrayList<Tuple3<EmptyParams, TrainingData, Iterable<Tuple2<Query, EmptyData>>>>();

    data.add(new Tuple3<EmptyParams, TrainingData, Iterable<Tuple2<Query, EmptyData>>>(
      new EmptyParams(),
      new TrainingData(ratings),
      new ArrayList<Tuple2<Query, EmptyData>>()
    ));

    return data;
  }

}