/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geosparql;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.LogCtlJUL;
import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.geosparql.spatial.SpatialIndexException;
import org.apache.jena.geosparql.spatial.index.v2.SpatialIndexIoKryo;
import org.apache.jena.query.Dataset ;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.system.AutoTxn;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;

import arq.cmdline.CmdARQ;

/**
 * Spatial indexer creation tool that reads a dataset and produces a spatial index
 * file from its quads.
 */
public class spatialindexer extends CmdARQ {
    public static final ArgDecl srsDecl = new ArgDecl(ArgDecl.HasValue, "srs") ;

    protected Path inFile;
    protected String srs ;
    protected Path outFile;

    static public void main(String... argv) {
        LogCtlJUL.routeJULtoSLF4J();
        new spatialindexer(argv).mainRun() ;
    }

    static public void testMain(String... argv) {
        new spatialindexer(argv).mainMethod() ;
    }

    protected spatialindexer(String[] argv) {
        super(argv) ;
        add(srsDecl);
    }

    @Override
    protected void processModulesAndArgs() {
        super.processModulesAndArgs();

        srs = getValue(srsDecl);

        List<String> positionals = getPositional();
        int n = positionals.size();

        if (n == 0) {
            throw new RuntimeException("No input TDB2 folder specified.");
        } else if (n > 1) {
            throw new RuntimeException("Too many positional arguments (" + n + ").");
        }

        String inFileStr = positionals.get(0);
        inFile = Path.of(inFileStr);

        String outFileStr = (n > 1)
            ? positionals.get(1)
            : "spatial.index";

        outFile = Path.of(outFileStr);
        outFile = outFile.resolveSibling(inFile).toAbsolutePath();
    }

    @Override
    protected String getSummary() {
        return getCommandName() + " tdb2-folder [--srs 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'] /path/to/tdb2 [spatial.index]" ;
    }

    @Override
    protected void exec() {
        if (!Files.exists(inFile)) {
            IO.exception(new FileNotFoundException("File not found: " + inFile));
        }

        if (Files.exists(outFile)) {
            IO.exception(new FileAlreadyExistsException("File already exists: " + outFile));
        }

        Location location = Location.create(inFile);
        Dataset dataset = TDB2Factory.connectDataset(location);
        try (AutoTxn txn = Txn.autoTxn(dataset, ReadWrite.READ)){
            SpatialIndexIoKryo.buildSpatialIndex(dataset, cmdName, outFile);
        } catch (SpatialIndexException e) {
            throw new RuntimeException(e);
        } finally {
            TDBInternal.expel(dataset.asDatasetGraph());
        }

        // Print the output filename.
        System.out.println(outFile);
    }
}

