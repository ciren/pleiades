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
    public List<Simulation> createSimulations(File xml, String fileKey) {
        List<Simulation> sims = new ArrayList<Simulation>();

        NodeList algorithms, problems, measurements, simulations;

        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xml);

            algorithms = doc.getElementsByTagName("algorithm");
            problems = doc.getElementsByTagName("problem");
            measurements = doc.getElementsByTagName("measurements");
            simulations = doc.getElementsByTagName("simulation");

            Document newJob;
            OutputStream cilibInput;

            String algID = null, prbID = null, msrID = null, curID = null;
            String jobOutputFileName, jobOutputPath;

            for (int s = 0; s < simulations.getLength(); s++) {
                Node alg = null, prb = null, msr = null, sim, output;
                Node simulation = simulations.item(s);

                if (simulation.getNodeType() == Node.ELEMENT_NODE) {
                    cilibInput = new ByteArrayOutputStream();
                    int samples = Integer.parseInt(simulation.getAttributes().getNamedItem("samples").getNodeValue());

                    newJob = dBuilder.newDocument();

                    sim = newJob.importNode(simulations.item(s), true);
                    sim.getAttributes().getNamedItem("samples").setNodeValue("1");
                    
                    NodeList children = simulations.item(s).getChildNodes();
                    for (int i = 0; i < children.getLength(); i++) {
                        Node child = simulations.item(s).getChildNodes().item(i);
                        
                        if (!child.hasAttributes() || child.getNodeName().equals("output")) {
                            continue;
                        }
                        
                        curID = child.getAttributes().getNamedItem("idref").getNodeValue();
                        
                        if (child.getNodeName().equals("algorithm")) {
                            algID = curID;
                        } else if (child.getNodeName().equals("problem")) {
                            prbID = curID;
                        } else if (child.getNodeName().equals("measurements")) {
                            msrID = curID;
                        }
                    }
                    
                    for (int i = 0; i < algorithms.getLength(); i++) {
                        if (algorithms.item(i).getAttributes().getNamedItem("id").getNodeValue().equals(algID)) {
                            alg = newJob.importNode(algorithms.item(i), true);
                            break;
                        }
                    }
                    
                    for (int i = 0; i < problems.getLength(); i++) {
                        if (problems.item(i).getAttributes().getNamedItem("id").getNodeValue().equals(prbID)) {
                            prb = newJob.importNode(problems.item(i), true);
                            break;
                        }
                    }
                    
                    for (int i = 0; i < measurements.getLength(); i++) {
                        if (measurements.item(i).getAttributes().getNamedItem("id").getNodeValue().equals(msrID)) {
                            msr = newJob.importNode(measurements.item(i), true);
                            break;
                        }
                    }

                    Element root = newJob.createElement("simulator");
                    Element algs = newJob.createElement("algorithms");
                    Element prbs = newJob.createElement("problems");
                    Element simsElement = newJob.createElement("simulations");

                    newJob.appendChild(root);
                    algs.appendChild(alg);
                    root.appendChild(algs);
                    prbs.appendChild(prb);
                    root.appendChild(prbs);
                    root.appendChild(msr);
                    simsElement.appendChild(sim);
                    root.appendChild(simsElement);

                    output = sim.getFirstChild();

                    while (!output.getNodeName().equals("output")) {
                         output = output.getNextSibling();
                    }

                    jobOutputPath = output.getAttributes().getNamedItem("file").getNodeValue();
                    if (jobOutputPath.endsWith("/")) {
                        jobOutputPath = jobOutputPath.substring(0, jobOutputPath.length() - 1);
                    }

                    jobOutputFileName = jobOutputPath;
                    if (jobOutputFileName.contains("/")) {
                        jobOutputFileName = jobOutputFileName.substring(jobOutputFileName.lastIndexOf("/") + 1);
                    }

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

        //System.out.println(sims.get(0).getCilibInput());
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
