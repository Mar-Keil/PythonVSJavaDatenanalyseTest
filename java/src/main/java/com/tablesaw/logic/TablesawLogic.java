package com.tablesaw.logic;

import static tech.tablesaw.aggregate.AggregateFunctions.count;
import static tech.tablesaw.aggregate.AggregateFunctions.sum;

import tech.tablesaw.api.Table;

public class TablesawLogic {
  public Table filter(Table flights) {
    return flights.where(flights.stringColumn("aircraft_model").isEqualTo("A319neo"));
  }

  public Table pivot(Table flights) {
    return flights.summarize("flight_distance", sum).by("aircraft_model");
  }

  public Table groupCount(Table flights) {
    return flights.summarize("flight_number", count).by("aircraft_model", "airline_code");
  }

  public Table join(Table flights, Table airlines) {
    return flights.joinOn("airline_code").inner(airlines);
  }

  public Table sort(Table flights) {
    return flights.sortAscendingOn("flight_number");
  }
}
