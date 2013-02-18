/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations.creator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import net.pleiades.simulations.CilibSimulation;
import net.pleiades.simulations.Simulation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author bennie
 */
public class XMLSimulationsCreator implements SimulationsCreator {
    private String id, owner, ownerEmail, name, releaseType;

    public XMLSimulationsCreator(String owner, String ownerEmail, String id, String name, String releaseType) {
        this.owner = owner;
        this.ownerEmail = ownerEmail;
        this.id = id;
        this.name = name;
        this.releaseType = releaseType;
    }


    @Override
    public List<Simulation> createSimulations(File xml, String fileKey/*byte[] run*/) {
        List<Simulation> sims = new ArrayList<Simulation>();

        NodeList algorithms, problems, measurements, simulations;

        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xml);

            algorithms = doc.getElementsByTagName("algorithms");
            problems = doc.getElementsByTagName("problems");
            measurements = doc.getElementsByTagName("measurements");
            simulations = doc.getElementsByTagName("simulation");

            Document newJob;
            OutputStream cilibInput;

            Node alg, prb, msr, sim, output;
            String jobOutputFileName, jobOutputPath;

            for (int s = 0; s < simulations.getLength(); s++) {
                Node simulation = simulations.item(s);

                if (simulation.getNodeType() == Node.ELEMENT_NODE) {
                    cilibInput = new ByteArrayOutputStream();
                    int samples = Integer.parseInt(simulation.getAttributes().getNamedItem("samples").getNodeValue());

                    newJob = dBuilder.newDocument();

                    alg = newJob.importNode(algorithms.item(0), true);
                    prb = newJob.importNode(problems.item(0), true);
                    msr = newJob.importNode(measurements.item(0), true);
                    sim = newJob.importNode(simulations.item(s), true);
                    sim.getAttributes().getNamedItem("samples").setNodeValue("1");

                    Element root = newJob.createElement("simulator");

                    newJob.appendChild(root);
                    root.appendChild(alg);
                    root.appendChild(prb);
                    root.appendChild(msr);
                    root.appendChild(sim);

                    output = sim.getFirstChild();

                    while (!output.getNodeName().equals("output")) {
                         output = output.getNextSibling();
                    }

                    jobOutputPath = output.getAttributes().getNamedItem("file").getNodeValue();
                    if (jobOutputPath.endsWith("/")) {
                        jobOutputPath = jobOutputPath.substring(0, jobOutputPath.length() - 1);
                    }
                    //System.out.println("\nOutputPath: " + jobOutputPath);

                    jobOutputFileName = jobOutputPath;
                    if (jobOutputFileName.contains("/")) {
                        jobOutputFileName = jobOutputFileName.substring(jobOutputFileName.lastIndexOf("/") + 1);
                    }
                    //System.out.println("\nOutputFileName: " + jobOutputFileName);

                    output.getAttributes().getNamedItem("file").setNodeValue("pleiades/" + s + ".pleiades");

                    TransformerFactory transformerFactory = TransformerFactory.newInstance();
                    Transformer transformer = transformerFactory.newTransformer();
                    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(cilibInput, "UTF-8"));
                    writer.println(createXMLDeclaration(newJob, "UTF-8"));
                    writer.println(createDTD(newJob));
                    writer.flush();

                    DOMSource source = new DOMSource(newJob);

                    transformer.transform(source, new StreamResult(cilibInput));

                    sims.add(new CilibSimulation(cilibInput.toString(), fileKey, jobOutputFileName, jobOutputPath, samples, owner, ownerEmail, id + "_" + s, name, releaseType));
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return sims;
    }

    private String createXMLDeclaration(Document doc, String charset) {
	return MessageFormat.format(
		"<?xml version=\"1.0\" encoding=\"{0}\" standalone=\"yes\" ?>",
		charset);
    }

    private String createDTD(Document doc) {
	return "<!DOCTYPE simulator [\n" +
                "<!ATTLIST algorithm id ID #IMPLIED>\n" +
                "<!ATTLIST problem id ID #IMPLIED>\n" +
                "<!ATTLIST measurements id ID #IMPLIED>\n" +
                "]>\n";
    }

}
