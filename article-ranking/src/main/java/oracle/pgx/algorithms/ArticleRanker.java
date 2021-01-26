/*
 ** Copyright (c) 2019, Oracle and/or its affiliates.  All rights reserved.
 ** Licensed under the Universal Permissive License v 1.0 as shown at http://oss.oracle.com/licenses/upl.
 */

package oracle.pgx.algorithms;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import oracle.pgx.api.CompiledProgram;
import oracle.pgx.api.Pgx;
import oracle.pgx.api.PgxGraph;
import oracle.pgx.api.PgxSession;
import oracle.pgx.api.PgxVertex;
import oracle.pgx.api.VertexProperty;
import oracle.pgx.common.types.PropertyType;
import oracle.pgx.config.FileGraphConfig;
import oracle.pgx.config.Format;
import oracle.pgx.config.GraphConfig;
import oracle.pgx.config.GraphConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.Files.lines;
import static oracle.pgx.algorithms.Utils.createOutputFile;
import static oracle.pgx.algorithms.Utils.getResource;
import static oracle.pgx.algorithms.Utils.limit;
import static oracle.pgx.algorithms.Utils.writeln;
import static oracle.pgx.algorithms.Utils.writer;

public class ArticleRanker {
  String szOPVFile = "../../data/connections.opv";
  String szOPEFile = "../../data/connections.ope";
  OraclePropertyGraph opg = OraclePropertyGraph.getInstance( args, szGraphName);
  opgdl = OraclePropertyGraphDataLoader.getInstance();
  opgdl.loadData(opg, szOPVFile, szOPEFile, 48 /* DOP */, 1000 /* batch size */, true /* rebuild index flag */, "pddl=t,pdml=t" /* options */);

  String statement =
          "CREATE PROPERTY GRAPH hr_simplified "
                  + "  VERTEX TABLES ( "
                  + "    hr.employees LABEL employee "
                  + "      PROPERTIES ARE ALL COLUMNS EXCEPT ( job_id, manager_id, department_id ), "
                  + "    hr.departments LABEL department "
                  + "      PROPERTIES ( department_id, department_name ) "
                  + "  ) "
                  + "  EDGE TABLES ( "
                  + "    hr.employees AS works_at "
                  + "      SOURCE KEY ( employee_id ) REFERENCES employees "
                  + "      DESTINATION departments "
                  + "      PROPERTIES ( employee_id ) "
                  + "  )";
  session.executePgql(statement);
  PgxGraph g = session.getGraph("HR_SIMPLIFIED");
  /**
   * Alternatively, one can use the prepared statement API, for example:
   */

  PgxPreparedStatement stmnt = session.preparePgql(statement);
  stmnt.execute();
  stmnt.close();
  PgxGraph g = session.getGraph("HR_SIMPLIFIED");

}