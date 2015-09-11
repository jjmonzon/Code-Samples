package com.armedica.onegate.datalayer.main;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;

import static com.armedica.onegate.datalayer.util.MapUtility.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.log4j.*;
import org.antlr.v4.runtime.tree.Tree;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.armedica.onegate.datalayer.caching.CacheAdaptor;
import com.armedica.onegate.datalayer.caching.HashableArray;
import com.armedica.onegate.datalayer.caching.HashableArrayList;
import com.armedica.onegate.datalayer.caching.ICacheUser;
import com.armedica.onegate.datalayer.common.SiebelUtils;
import com.armedica.onegate.datalayer.logging.OgLogger;
import com.armedica.onegate.datalayer.logging.OgLoggerFactory;
import com.armedica.onegate.datalayer.main.ChangeCaptureEngine.RuleContext;
import static com.armedica.onegate.datalayer.main.SpawnLink.logger;
import com.armedica.onegate.datalayer.util.CollectionUtility;
import com.armedica.onegate.datalayer.util.DateUtils;
import com.armedica.onegate.datalayer.util.SiebelTypesUtility;
import com.armedica.onegate.datalayer.util.StringUtility;
import static com.armedica.onegate.datalayer.util.StringUtility.isEmpty;
import static com.armedica.onegate.datalayer.util.StringUtility.notEmpty;
import com.armedica.onegate.datalayer.util.XmlUtility;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.threeten.bp.LocalDate;

import org.w3c.dom.Document;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Attr;
import org.w3c.dom.Text;

 /**
 * Internal class that allows us to create our own internal xpath language to traverse logical trees in our object model.
 *
 * @author Josh Monzon
 *
 */
public class DataLayerXPath {

    final static Logger logger = Logger.getLogger(DataLayerXPath.class);

    /**
     * We need the ability to traverse our expressions to see if we even want to
     * execute them. This is the base class for anything that can appear in an
     * expression. We are building this class off the antlr tree structure
     * because we will soon be using antlr to parse expressions into the tree
     * structure for us.
     *
     * @author Josh Monzon
     *
     */
    public static class OgTree implements org.antlr.v4.runtime.tree.Tree {

        public List<OgTree> children = new ArrayList<OgTree>();
        public OgTree parent;

        public OgTree() {
            this.parent = null;
        }

        public OgTree(OgTree parent) {
            this.parent = parent;
        }

        public void addChild(OgTree child) {
            this.children.add(child);
        }

        public void addChildren(OgTree... children) {
            for (OgTree child : children) {
                this.children.add(child);
            }
        }

        public void addChildren(Iterable<OgTree> children) {
            for (OgTree child : children) {
                this.children.add(child);
            }
        }

        @Override
        public Tree getChild(int arg0) {
            return children.get(arg0);
        }

        @Override
        public int getChildCount() {
            return children.size();
        }

        @Override
        public Tree getParent() {
            return this.getParent();
        }

        @Override
        public Object getPayload() {
            return this;
        }

        @Override
        public String toStringTree() {
            // maybe do something here?
            return null;
        }

        public List<OgTree> getOgTreeDepthFirstList() {
            List<OgTree> list = new ArrayList<OgTree>();
            Iterator<OgTree> iter = this.getOgTreeDepthFirstIterator();
            while (iter.hasNext()) {
                list.add(iter.next());
            }
            return list;
        }

        public Iterator<OgTree> getOgTreeDepthFirstIterator() {
            List<OgTree> list = new ArrayList<OgTree>();
            list.add(this);
            return new OgTreeDepthFirstIter(list.iterator());
        }

        public Iterator<OgTree> getOgTreeDepthFirstChildIterator() {
            return new OgTreeDepthFirstIter(this.children.iterator());
        }

        public static class OgTreeDepthFirstIter implements Iterator<OgTree> {

            final Iterator<OgTree> inputNodes;
            OgTree inputNode;
            Iterator<OgTree> childNodes = null;

            public OgTreeDepthFirstIter(Iterator<OgTree> input) {
                this.inputNodes = input;
            }

            @Override
            public boolean hasNext() {
                // return input children, then input.
                for (;;) {
                    if (childNodes != null && childNodes.hasNext()) {
                        return true;
                    }
                    if (this.inputNode != null) {
                        return true;
                    }
                    if (!inputNodes.hasNext()) {
                        return false;
                    }
                    this.inputNode = inputNodes.next();
                    if (this.inputNode.getChildCount() > 0) {
                        this.childNodes = new OgTreeDepthFirstIter(this.inputNode.children.iterator());
                    }
                }
            }

            @Override
            public OgTree next() {
                if (!this.hasNext()) {
                    return null;
                }
                if (this.childNodes != null && this.childNodes.hasNext()) {
                    return this.childNodes.next();
                }
                if (this.inputNode != null) {
                    OgTree result = this.inputNode;
                    this.inputNode = null;
                    return result;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

    public abstract class BinaryBooleanOp extends BooleanOp {

        String name;

        public OgTree getLeft() {
            return (OgTree) this.getChild(0);
        }

        public OgTree getRight() {
            return (OgTree) this.getChild(1);
        }

        public BinaryBooleanOp(String name, OgTree left, OgTree right) {
            this.name = name;
            this.addChild(left);
            this.addChild(right);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static abstract class BooleanOp extends OgTree {

        public BooleanOp() {
            super();
        }

        public BooleanOp(OgTree parent) {
            super(parent);
        }

        public abstract boolean evaluate(DLNode contextNode, DLContext context);
    }

    /**
     * Iterator base class for building things that return at most 1 node sum(),
     * count(), first(), last()
     *
     * @author Josh Monzon
     */
    public abstract static class XPathAggregateIter implements Iterator<DLNode> {

        Iterator<DLNode> input;
        int inputPosition = 0;
        DLContext context;

        public XPathAggregateIter(Iterator<DLNode> input, DLContext context) {
            this.input = input;
            this.context = context;
        }

        @Override
        public boolean hasNext() {
            return inputPosition == 0 && input.hasNext();
        }

        @Override
        public DLNode next() {
            if (!this.hasNext()) {
                return null;
            } else {
                return this.getOutput();
            }
        }

        protected DLNode nextInput() {
            if (this.input.hasNext()) {
                this.inputPosition++;
                return input.next();
            }
            return null;
        }

        abstract DLNode getOutput();

        @Override
        public void remove() {
            throw new RuntimeException("not implemented");
        }
    }

    /**
     * Base class for xpaths that needs to scan the set and return a set.
     *
     * @author Josh Monzon
     *
     */
    public abstract static class XPathSetIter implements Iterator<DLNode> {

        private Iterator<DLNode> input;
        DLContext context;
        private List<DLNode> outputSet;
        private List<DLNode> inputSet;
        private Iterator<DLNode> output;

        public XPathSetIter(Iterator<DLNode> input, DLContext context) {
            this.input = input;
            this.context = context;
        }

        /**
         * Given the entire input set return the entire output set
         *
         * @param inputSet
         * @return
         */
        abstract List<DLNode> getOutputSet(List<DLNode> inputSet);

        @Override
        public boolean hasNext() {
            if (this.output == null) {
                this.inputSet = new ArrayList<DLNode>();
                while (this.input.hasNext()) {
                    this.inputSet.add(this.input.next());
                }
                this.outputSet = this.getOutputSet(this.inputSet);
                this.output = this.outputSet.iterator();
            }
            return this.output.hasNext();
        }

        @Override
        public DLNode next() {
            if (!this.hasNext()) {
                return null;
            } else {
                return this.output.next();
            }
        }

        @Override
        public void remove() {
            throw new RuntimeException("not implemented");
        }
    }

    public abstract static class PassThruIter implements Iterator<DLNode> {

        Iterator<DLNode> input;
        DLContext context;
        Queue<DLNode> output = new LinkedList<DLNode>();

        public PassThruIter(Iterator<DLNode> input, DLContext context) {
            this.input = input;
            this.context = context;
        }

        public abstract void run(DLNode ogEntityNode, DLContext context);

        @Override
        public boolean hasNext() {
            return this.input.hasNext();
        }

        @Override
        public DLNode next() {
            if (this.hasNext()) {
                DLNode ogEntityNode = this.input.next();
                run(ogEntityNode, context);
                return ogEntityNode;
            } else {
                return null;
            }
        }

        @Override
        public void remove() {
            throw new RuntimeException("not implemented");
        }
    }

    /**
     * This this is the base class for some XPath iterators.
     *
     * @author Josh Monzon
     *
     */
    public abstract static class XPathIter implements Iterator<DLNode> {

        Iterator<DLNode> input;
        DLContext context;
        Queue<DLNode> output = new LinkedList<DLNode>();

        public XPathIter(Iterator<DLNode> input, DLContext context) {
            this.input = input;
            this.context = context;
        }

        /**
         * Continues to read from input and write to output until output is not
         * empty or input is empty. Returns true if there is something output.
         */
        @Override
        public abstract boolean hasNext();

        @Override
        public DLNode next() {
            if (this.hasNext()) {
                return output.remove();
            } else {
                return null;
            }
        }

        @Override
        public void remove() {
            throw new RuntimeException("not implemented");
        }
    }

    /**
     * This this is the base class for shallow (1 level deep) XPath iterators.
     * It returns the immediate children of each input.
     *
     * @author Josh Monzon
     *
     */
    public abstract static class XPathImmediateChildIter extends XPathIter {

        public XPathImmediateChildIter(Iterator<DLNode> input, DLContext context) {
            super(input, context);
        }

        public abstract List<DLNode> selectShallow(DLNode contextNode);

        @Override
        public boolean hasNext() {
            // logger.debug("hasNext for " + this.getClass().getSimpleName());
            while (this.output.isEmpty() && input.hasNext()) {
                // logger.debug("..in while loop");
                DLNode contextNode = input.next();
                this.output.addAll(this.selectShallow(contextNode));
            }
            boolean hasNext = !this.output.isEmpty();
            // logger.debug("..returning " + hasNext);
            return hasNext;
        }
    }

    /**
     * This this is the base class for depth first (N levels deep) XPath
     * iterators. Returns all children (depth first) of each input.
     *
     * Predicates are used to filter the results of path expressions. The
     * sequences that are returned by path expressions have an inherent and
     * stable order, which is called document order. In XPath, document order is
     * defined as depth first, which means that when the system has to return
     * nodes in order, it looks down before it looks right, it never looks up
     * (except to resume where it left off), and it never looks left.
     *
     * http://clover.slavic.pitt.edu/humcomp/introduction-xpath.xhtml
     *
     * @author Josh Monzon
     *
     */
    public static class XPathChildDepthFirstRecursiveIter implements Iterator<DLNode> {

        final Iterator<DLNode> input;
        DLNode inputNode;
        Iterator<DLNode> output = null;

        public XPathChildDepthFirstRecursiveIter(Iterator<DLNode> input) {
            this.input = input;
        }

        @Override
        public boolean hasNext() {
            for (;;) {
                if (output != null && output.hasNext()) {
                    return true;
                }
                if (!input.hasNext()) {
                    return false;
                }
                this.inputNode = input.next();
                output = new DLNodeBase.ChildDepthFirstIterator(this.inputNode);
            }
        }

        @Override
        public DLNode next() {
            if (!this.hasNext()) {
                return null;
            }
            return this.output.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Base class for all xpath operators
     *
     * @author Josh Monzon
     *
     */
    public abstract static class XPathOp extends OgTree {

        public XPathOp() {
        }

        public String toTagName(String input) {
            return StringUtility.toJavaName(input);
        }

        public String[] toTagName(String... input) {
            String[] output = new String[input.length];
            for (int i = 0; i < input.length; i++) {
                output[i] = StringUtility.toJavaName(input[i]);
            }
            return output;
        }

        public List<String> toTagName(List<String> input) {
            List<String> output = new ArrayList<>(input.size());
            for (int i = 0; i < input.size(); i++) {
                output.add(StringUtility.toJavaName(input.get(i)));
            }
            return output;
        }

        public abstract Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input);

    }

    /**
     * Represents a 'compiled' xpath expression consisting of pipelined xpath
     * operators. An xpath is ALSO an XPathOp. In this case it represents a
     * subquery.
     *
     * @author Josh Monzon
     *
     */
    public static class DLXPath extends XPathOp {

        XPathOp[] ops;

        // append constructor, creates a new xpath by appending to an existing
        // one
        public DLXPath(DLXPath xpath, XPathOp... ops) {
            this((XPathOp[]) ArrayUtils.addAll(xpath.ops, ops));
        }

        public DLXPath(XPathOp... ops) {
            // logger.debug("INSTANTIATE XPath with numOPs=" + ops.length);
            this.ops = ops;
            this.addChildren(ops);
        }

        // this traverses the xpath and returns true if it methods any of the
        // passed items
        public boolean containsAttributeNameMatch(String... nameMatches) {
            if (this.ops == null) {
                return false;
            }
            for (OgTree op : this.ops) {
                for (OgTree n : op.getOgTreeDepthFirstList()) {
                    if (n instanceof Attribute) {
                        Attribute a = (Attribute) n;
                        if (StringUtility.in(a.nameMatch, nameMatches)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        // get an iterator given the default context for the passed node
        public Iterator<DLNode> iterator(DLNode contextNode) {
            DLContext context = new DLContext(contextNode);
            return this.iterator(context, contextNode);
        }

        // select and materialize all node given the default context for the
        // passed node
        public List<DLNode> select(DLNode contextNode) {
            DLContext context = new DLContext(contextNode);
            return this.select(context, contextNode);
        }

        // get an iterator to the xpath results given the passed context
        public Iterator<DLNode> iterator(DLContext context, DLNode contextNode) {
            Iterator<DLNode> input = contextNode.getSelfIterator();
            // logger.debug("BEGIN XPATH ON NODE:"+contextNode.toXmlString());
            if (ops.length == 0) {
                return input;
            }
            for (XPathOp op : ops) {
                // logger.debug("pass op " + op.getClass() +
                // " iterator of type " + input.getClass());
                input = op.iterator(context, input);
                // set the dynamic context
            }
            // logger.debug("XPath retuning iterator of type " +
            // input.getClass());
            return input;
        }

        // select and materialize all xpath results given the passed context
        public List<DLNode> select(DLContext context, DLNode contextNode) {
            try {
                Iterator<DLNode> iter = this.iterator(context, contextNode);
                List<DLNode> output = new ArrayList<DLNode>();
                while (iter.hasNext()) {
                    output.add(iter.next());
                }
                return output;
            } catch (Exception e) {
                throw new RuntimeException("Error evaluating xpath from context node [" + contextNode.getTagName() + "] path=[" + this.toString() + "]", e);
            }
        }

        // ////////////////// this is how you treat an xpath as an operator
        // ////////////
        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("xpath(");
            for (int i = 0; i < ops.length; i++) {
                XPathOp op = this.ops[i];
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(op.toString());
            }
            sb.append(")");
            return sb.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            // logger.debug("Calling xpathOp iterator..");
            return new XPathOpIterator(input, context);
        }

        // this is intentionally not static as it uses the
        // reference to the enclosing xpath.
        private class XPathOpIterator implements Iterator<DLNode> {

            Iterator<DLNode> input;
            DLContext context;
            Iterator<DLNode> output;

            public XPathOpIterator(Iterator<DLNode> input, DLContext context) {
                this.input = input;
                this.context = context;
                // logger.debug("Calling xpathOp iterator CONSTRUCTOR..");
            }

            @Override
            public boolean hasNext() {
                for (;;) {
                    // if we have an output already, return true
                    if (output != null) {
                        if (output.hasNext()) {
                            return true;
                        } else {
                            output = null;
                        }
                    }
                    // if our input still has data, get more output
                    if (input.hasNext()) {

                        DLNode inputNode = input.next();
                        // logger.debug("XPATH pipe in "+inputNode.toString());
                        output = DLXPath.this.iterator(context, inputNode);
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public DLNode next() {
                if (this.hasNext()) {
                    return this.output.next();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

    public static XPathOp select(XPathOp op) {
        return new DLXPath(op);
    }

    public static XPathOp select(XPathOp... ops) {
        return new DLXPath(ops);
    }

    public static XPathOp select(DLXPath xpath, XPathOp... ops) {
        return new DLXPath(xpath, ops);
    }

    public static DLXPath xpath(XPathOp op) {
        return new DLXPath(op);
    }

    public static DLXPath xpath(XPathOp... ops) {
        return new DLXPath(ops);
    }

    public static DLXPath xpath(DLXPath xpath, XPathOp... ops) {
        return new DLXPath(xpath, ops);
    }

    // append ops to an existing path to create a new one
    public static DLXPath xpathOverload(DLXPath xpath, XPathOp... ops) {
        return new DLXPath(xpath, ops);
    }

    /**
     * 'left/' Child operator; selects immediate children of the left-side
     * collection.
     *
     * @author Josh Monzon
     */
    public static class Child extends XPathOp {

        String[] nameMatches;

        public Child(String... nameMatches) {
            this.nameMatches = nameMatches;
        }

        @Override
        public String toString() {
            return "child(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public String toStringTree() {
            return "child(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ChildIterator(input, context, this.nameMatches);
        }

        public static class ChildIterator extends XPathImmediateChildIter {

            public String[] nameMatches;

            public ChildIterator(Iterator<DLNode> input, DLContext context, String[] nameMatches) {
                super(input, context);
                this.nameMatches = nameMatches;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                // logger.debug("Child selectShallow on " +
                // contextNode.toXmlString());
                List<DLNode> list = new ArrayList<DLNode>();
                for (String nameMatch : nameMatches) {
                    if (nameMatch.equals("*")) {
                        list.addAll(contextNode.getChildNodes(context));
                    } else {
                        list.addAll(contextNode.getChildNodes(context, nameMatch));
                    }
                }
                //logger.info("["+contextNode.getTagName()+"]->child("+Joiner.on(",").join(nameMatches)+") returning :"+Joiner.on(",").join(DLNodeBase.getTagNames(list)));
                return list;
            }
        }
    }

    public static Child child(String nameMatches) {
        return new Child(new String[]{nameMatches});
    }

    public static Child child(String... nameMatches) {
        return new Child(nameMatches);
    }

    /////////////////////////////
    /**
     * 'left/' ChildEntities operator; selects all child nodes of the given node
     * name collection.
     *
     * @author Josh Monzon
     */
    public static class ChildEntities extends XPathOp {

        String[] nameMatches;

        public ChildEntities(String... nameMatches) {
            this.nameMatches = nameMatches;
        }

        @Override
        public String toString() {
            return "ChildEntities(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public String toStringTree() {
            return "ChildEntities(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ChildEntitiesIterator(input, context, this.nameMatches);
        }

        public static class ChildEntitiesIterator extends XPathImmediateChildIter {

            public String[] nameMatches;

            public ChildEntitiesIterator(Iterator<DLNode> input, DLContext context, String[] nameMatches) {
                super(input, context);
                this.nameMatches = nameMatches;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                // logger.debug("ChildEntities selectShallow on " +
                // contextNode.toXmlString());
                List<DLNode> list = new ArrayList<DLNode>();
                for (String nameMatch : nameMatches) {
                    List<DLNode> childNodes;
                    if (nameMatch.equals("*")) {
                        childNodes = contextNode.getChildNodes(context);
                    } else {
                        childNodes = contextNode.getChildNodes(context, nameMatch);
                    }

                    for (DLNode childNode : childNodes) {
                        list.addAll(childNode.getChildNodes(context));
                    }
                }

                //logger.info("["+contextNode.getTagName()+"]->ChildEntities("+Joiner.on(",").join(nameMatches)+") returning :"+Joiner.on(",").join(DLNodeBase.getTagNames(list)));
                return list;
            }
        }
    }

    public static ChildEntities childEntities(String... nameMatches) {
        return new ChildEntities(nameMatches);
    }

    /////////////////////////////
    public static class MaterializedChild extends XPathOp {

        String[] nameMatches;

        public MaterializedChild(String... nameMatches) {
            this.nameMatches = nameMatches;
        }

        @Override
        public String toString() {
            return "materialized(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public String toStringTree() {
            return "child(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new MaterializedChildIterator(input, context, this.nameMatches);
        }

        public static class MaterializedChildIterator extends XPathImmediateChildIter {

            public String[] nameMatches;

            public MaterializedChildIterator(Iterator<DLNode> input, DLContext context, String[] nameMatches) {
                super(input, context);
                this.nameMatches = nameMatches;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                // logger.debug("Child selectShallow on " +
                // contextNode.toXmlString());
                List<DLNode> list = new ArrayList<DLNode>();
                for (String nameMatch : nameMatches) {
                    if (nameMatch.equals("*")) {
                        list.addAll(contextNode.getMaterializedChildNodes(context));
                    } else {
                        list.addAll(contextNode.getMaterializedChildNodes(context, nameMatch));
                    }
                }
                //logger.info("["+contextNode.getTagName()+"]->child("+Joiner.on(",").join(nameMatches)+") returning :"+Joiner.on(",").join(DLNodeBase.getTagNames(list)));
                return list;
            }
        }
    }

    public static MaterializedChild materialized(String nameMatches) {
        return new MaterializedChild(new String[]{nameMatches});
    }

    public static MaterializedChild materialized(String... nameMatches) {
        return new MaterializedChild(nameMatches);
    }

    //////////////////////////////
    /**
     * This operator touches the child operand (selects it) but just returns its
     * input. This is mostly used for testing the datalayer.
     *
     * @author Josh Monzon
     */
    public static class TouchChild extends XPathOp {

        String[] nameMatches;

        public TouchChild(String... nameMatches) {
            this.nameMatches = nameMatches;
        }

        @Override
        public String toString() {
            return "touchChild(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public String toStringTree() {
            return "touchChild(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new TouchChildIterator(input, context, this.nameMatches);
        }

        public static class TouchChildIterator extends XPathImmediateChildIter {

            public String[] nameMatches;

            public TouchChildIterator(Iterator<DLNode> input, DLContext context, String[] nameMatches) {
                super(input, context);
                this.nameMatches = nameMatches;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                // logger.debug("TouchChild selectShallow on " +
                // contextNode.toXmlString());
                List<DLNode> list = new ArrayList<DLNode>();
                for (String nameMatch : nameMatches) {
                    if (nameMatch.equals("*")) {
                        contextNode.getChildNodes(context);
                    } else {
                        contextNode.getChildNodes(context, nameMatch);
                    }
                }
                list.add(contextNode);
                return list;
            }
        }
    }

    public static TouchChild touchChild(String nameMatches) {
        return new TouchChild(new String[]{nameMatches});
    }

    public static TouchChild touchChild(String... nameMatches) {
        return new TouchChild(nameMatches);
    }

    // //////////////////////////////////////////////////////////////
    /**
     * This operator takes pipeline args like an xpath but returns the current
     * node. This is mostly used for testing the datalayer.
     *
     * @author Josh Monzon
     */
    public static class Touch extends XPathOp {

        DLXPath touchXPath;

        public Touch(XPathOp... ops) {
            this.touchXPath = xpath(ops);
        }

        @Override
        public String toString() {
            return "touchXPath(" + this.touchXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new TouchIterator(input, context, this.touchXPath);
        }

        public static class TouchIterator extends XPathImmediateChildIter {

            public DLXPath nameMatches;

            public TouchIterator(Iterator<DLNode> input, DLContext context, DLXPath nameMatches) {
                super(input, context);
                this.nameMatches = nameMatches;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                // logger.debug("Touch selectShallow on " +
                // contextNode.toXmlString());
                List<DLNode> list = new ArrayList<DLNode>();
                contextNode.select(context, nameMatches);
                list.add(contextNode);
                return list;
            }
        }
    }

    public static Touch touchXPath(XPathOp... ops) {
        return new Touch(ops);
    }

    public static class Original extends XPathOp {

        public final String nameMatch = "old_value";

        public Original() {
        }

        @Override
        public String toString() {
            return "original()";
        }

        @Override
        public String toStringTree() {
            return "original()";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new OriginalIterator(input, context, this.nameMatch);
        }

        public static class OriginalIterator extends XPathImmediateChildIter {

            public String nameMatch;

            public OriginalIterator(Iterator<DLNode> input, DLContext context, String nameMatch) {
                super(input, context);
                this.nameMatch = nameMatch;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<>();
                if (!(contextNode instanceof DLElementBase)) {
                    return list;
                }
                DLElement contextElement = (DLElement) contextNode;
                if (contextElement.hasAttribute(nameMatch)) {
                    DLNode attributeNode = contextElement.getAttributeNode(nameMatch);
                    list.add(attributeNode);
                } else {
                    list.add(contextNode);
                }
                return list;
            }
        }
    }

    /**
     * If context node has old_value it returns that attribute else it returns
     * the current node.
     *
     * @param nameMatch
     * @return
     */
    public static Original original() {
        return new Original();
    }

    // //////////////////////////////////////////////////////////////
    public static class Attribute extends XPathOp {

        public final String nameMatch;

        public Attribute(String nameMatch) {
            this.nameMatch = nameMatch;
        }

        @Override
        public String toString() {
            return "attribute(" + nameMatch + ")";
        }

        @Override
        public String toStringTree() {
            return "attribute(" + nameMatch + ")";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new AttributeIterator(input, context, this.nameMatch);
        }

        public static class AttributeIterator extends XPathImmediateChildIter {

            public String nameMatch;

            public AttributeIterator(Iterator<DLNode> input, DLContext context, String nameMatch) {
                super(input, context);
                this.nameMatch = nameMatch;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                // logger.debug("Child selectShallow on " +
                // contextNode.toXmlString());
                List<DLNode> list = new ArrayList<DLNode>();
                if (!(contextNode instanceof DLElementBase)) {
                    return list;
                }
                DLElement contextElement = (DLElement) contextNode;
                if (nameMatch.equals("*")) {
                    list.addAll(contextElement.getAttributeNodes());
                } else {
                    DLNode attributeNode = contextElement.getAttributeNode(nameMatch);
                    if (attributeNode != null) {
                        list.add(attributeNode);
                    }
                }
                return list;
            }
        }
    }

    public static Attribute attribute(String nameMatch) {
        return new Attribute(nameMatch);
    }

    // //////////////////////////////////////////////////////////////
    /**
     * 'left//' Recursive descent; searches for the specified element 'left' at
     * any depth below current context.
     * http://msdn.microsoft.com/en-us/library/ms256122(v=vs.110).aspx
     *
     * @author Josh Monzon
     *
     */
    public static class RecursiveDescent extends XPathOp {

        String nameMatch;

        public RecursiveDescent(String nameMatch) {
            this.nameMatch = nameMatch;
        }

        @Override
        public String toString() {
            return "recursiveDescent(" + nameMatch + ")";
        }

        @Override
        public String toStringTree() {
            return "recursiveDescent(" + nameMatch + ")";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new RecursiveDescentIterator(input, this.nameMatch);
        }

        public static class RecursiveDescentIterator implements Iterator<DLNode> {

            public String nameMatch;
            XPathChildDepthFirstRecursiveIter recursiveIter;
            DLNode nextNode = null;

            public RecursiveDescentIterator(Iterator<DLNode> input, String nameMatch) {
                this.nameMatch = nameMatch;
                this.recursiveIter = new XPathChildDepthFirstRecursiveIter(input);
            }

            @Override
            public boolean hasNext() {
                for (;;) {
                    if (this.nextNode != null) {
                        return true;
                    }
                    if (!this.recursiveIter.hasNext()) {
                        return false;
                    }
                    this.nextNode = this.recursiveIter.next();
                    // logger.debug("NextNode="+this.nextNode.getTagName());
                    String nextNodeTagName = this.nextNode.getTagName();
                    if (!nameMatch.equals("*") && !nameMatch.equals(nextNodeTagName)) {
                        this.nextNode = null;
                    }
                }

            }

            @Override
            public DLNode next() {
                if (this.hasNext()) {
                    DLNode returnNode = this.nextNode;
                    this.nextNode = null;
                    return returnNode;
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();

            }
        }
    }

    public static RecursiveDescent recursiveDescent(String nameMatch) {
        return new RecursiveDescent(nameMatch);
    }

    // ////////////
    /**
     * '..' The parent of the current context node.
     * http://msdn.microsoft.com/en-us/library/ms256122(v=vs.110).aspx
     *
     * @author Josh Monzon
     *
     */
    public static class Parent extends XPathOp {

        public Parent() {
        }

        @Override
        public String toString() {
            return "parent()";
        }

        @Override
        public String toStringTree() {
            return "parent()";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ParentIterator(input, context);
        }

        public static class ParentIterator extends XPathImmediateChildIter {

            public ParentIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();
                if (contextNode.getParentNode() != null) {
                    list.add(contextNode.getParentNode());
                }
                return list;
            }
        }
    }

    public static Parent parent() {
        return new Parent();
    }

    // ///////////////////
    /**
     * This is like parent operator but it validates the tag name
     *
     * @author Josh Monzon
     *
     */
    public static class ParentElement extends XPathOp {

        String[] nameMatches;

        public ParentElement(String... nameMatches) {
            this.nameMatches = nameMatches;
        }

        @Override
        public String toString() {
            return "parent(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ParentElementIterator(input, context, this.nameMatches);
        }

        public static class ParentElementIterator extends XPathImmediateChildIter {

            String[] nameMatches;

            public ParentElementIterator(Iterator<DLNode> input, DLContext context, String[] nameMatches) {
                super(input, context);
                this.nameMatches = nameMatches;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();
                DLNode parentNode = contextNode.getParentNode();
                if (parentNode != null) {
                    for (String nameMatch : nameMatches) {
                        if (nameMatch.equals("*")) {
                            list.add(parentNode);
                            break;
                        } else if (nameMatch.equals(parentNode.getTagName())) {
                            list.add(parentNode);
                            break;
                        }
                    }
                }
                return list;
            }
        }
    }

    public static ParentElement parent(String... tagNames) {
        return new ParentElement(tagNames);
    }

    // ////////////////
    /**
     * Looks 1 or 2 levels up for a an entity element that matches the passed
     * names or any if no name is passed.
     *
     * @author Josh Monzon
     *
     */
    public static class ParentEntity extends XPathOp {

        String[] nameMatches;

        public ParentEntity(String... nameMatches) {
            this.nameMatches = nameMatches;
        }

        @Override
        public String toString() {
            return "parentEntity(" + StringUtils.join(this.nameMatches, ',') + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ParentEntityIterator(input, context, this.nameMatches);
        }

        public static class ParentEntityIterator extends XPathImmediateChildIter {

            String[] nameMatches;

            public ParentEntityIterator(Iterator<DLNode> input, DLContext context, String[] nameMatches) {
                super(input, context);
                this.nameMatches = nameMatches;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();
                DLNode parentNode = null;
                // parent of an entity needs to lookup up 2 levels
                if (contextNode instanceof DLEntity) {
                    parentNode = contextNode.getParentNode();
                    if (parentNode != null && parentNode instanceof DLLink) {
                        parentNode = parentNode.getParentNode();
                    }
                } else {
                    parentNode = contextNode.getParentNode();
                }
                if (parentNode != null && parentNode instanceof DLEntity) {
                    for (String nameMatch : nameMatches) {
                        if (nameMatch.equals("*")) {
                            list.add(parentNode);
                            break;
                        } else if (nameMatch.equals(parentNode.getTagName())) {
                            list.add(parentNode);
                            break;
                        }
                    }
                }
                return list;
            }
        }
    }

    public static ParentEntity parentEntity(String... tagNames) {
        return new ParentEntity(tagNames);
    }

    // ////////////////
    /**
     * '..' The parent of the current context node.
     * http://msdn.microsoft.com/en-us/library/ms256122(v=vs.110).aspx
     *
     * @author Josh Monzon
     *
     */
    public static class CurrentContext extends XPathOp {

        public CurrentContext() {
        }

        @Override
        public String toString() {
            return "currentContext()";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new CurrentContextIterator(input, context);
        }

        public static class CurrentContextIterator extends XPathImmediateChildIter {

            public CurrentContextIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>(1);
                list.add(contextNode);
                return list;
            }
        }
    }

    public static CurrentContext currentContext() {
        return new CurrentContext();
    }

    public static class Root extends XPathOp {

        String name;

        public Root(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "root(" + name + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new RootIterator(input, context, name);
        }

        public static class RootIterator extends XPathImmediateChildIter {

            String name;

            public RootIterator(Iterator<DLNode> input, DLContext context, String name) {
                super(input, context);
                this.name = name;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();
                DLNode root = null;
                if (context != null) {
                    root = context.rootNode;
                }
                if (root == null) {
                    root = contextNode.getRootNode();
                }
                if (root != null && name.equals(root.getTagName())) {
                    list.add(root);
                }
                return list;
            }
        }
    }

    public static Root root(String name) {
        return new Root(name);
    }

    public static class Variable extends XPathOp {

        String name;

        public Variable(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "var(" + name + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new VariableIterator(input, context, name);
        }

        public static class VariableIterator extends XPathImmediateChildIter {

            String name;

            public VariableIterator(Iterator<DLNode> input, DLContext context, String name) {
                super(input, context);
                this.name = name;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                return this.context.getVariable(this.name);
            }
        }
    }

    public static Variable var(String name) {
        return new Variable(name);
    }

    public static class GetNextUID extends XPathOp {

        public GetNextUID() {
        }

        @Override
        public String toString() {
            return "getNextUID()";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new GetNextUIDIterator(input, context);
        }

        public static class GetNextUIDIterator extends XPathImmediateChildIter {

            String name;

            public GetNextUIDIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> output = new ArrayList<DLNode>(1);
                output.add(new DLValue(this.context.dataLayerSlice.dataLayer.getNextUID().toString()));
                return output;
            }
        }
    }

    public static GetNextUID getNextUID() {
        return new GetNextUID();
    }

    public static class GetDate extends XPathOp {

        public GetDate() {
        }

        @Override
        public String toString() {
            return "getDate()";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new GetDateIterator(input, context);
        }

        public static class GetDateIterator extends XPathImmediateChildIter {

            String name;

            public GetDateIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                Date now = new Date();
                List<DLNode> output = new ArrayList<DLNode>(1);
                output.add(new DLValue(dateToString(now)));
                return output;
            }
        }
    }

    public static GetDate getDate() {
        return new GetDate();
    }

    public static String getDateString() {
        Date now = new Date();
        return dateToString(now);
    }

    /**
     * '[i]' Subscript operator; used for indexing within a collection. Index
     * starts a 1 for first element.
     * http://msdn.microsoft.com/en-us/library/ms256122(v=vs.110).aspx
     *
     * @author Josh Monzon
     */
    public static class Subscript extends XPathOp {

        int index;

        public Subscript(int index) {
            this.index = index;
        }

        @Override
        public String toString() {
            return "subscript(" + index + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new SubscriptIterator(input, context, this.index);
        }

        public static class SubscriptIterator extends XPathImmediateChildIter {

            public int index;
            public int currIndex = 0;

            public SubscriptIterator(Iterator<DLNode> input, DLContext context, int index) {
                super(input, context);
                this.index = index;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                this.currIndex++;
                // logger.debug("Subscript selectShallow on " +
                // contextNode+", currIndex="+this.currIndex);
                List<DLNode> list = new ArrayList<DLNode>();
                if (this.currIndex == index) {
                    list.add(contextNode);
                }
                return list;
            }
        }
    }

    public static Subscript subscript(int index) {
        return new Subscript(index);
    }

    // /////////////////////////
    public static class UnionAll extends XPathOp {

        DLXPath[] xpaths;

        public UnionAll(DLXPath... xpaths) {
            this.xpaths = xpaths;
            for (DLXPath xpath : this.xpaths) {
                this.addChild(xpath);
            }
        }

        @Override
        public String toString() {
            return "union_all(" + StringUtils.join(xpaths, ",") + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new UnionAllIterator(input, context, this.xpaths);
        }

        public static class UnionAllIterator implements Iterator<DLNode> {

            DLContext context;
            Iterator<DLNode> input;
            public DLXPath[] xpaths;
            public int xpathsIdx = -1;
            Iterator<DLNode> output;
            DLNode curInput;

            public UnionAllIterator(Iterator<DLNode> input, DLContext context, DLXPath... xpaths) {
                this.input = input;
                this.context = context;
                this.xpaths = xpaths;
            }

            @Override
            public boolean hasNext() {
                for (;;) {
                    if (output != null && output.hasNext()) {
                        return true;
                    }
                    if (curInput != null && (xpathsIdx + 1) < xpaths.length) {
                        xpathsIdx++;
                        output = curInput.selectIterator(context, xpaths[xpathsIdx]);
                        continue;
                    }
                    if (input.hasNext()) {
                        this.curInput = this.input.next();
                        this.xpathsIdx = -1;
                        continue;
                    }
                    return false;
                }
            }

            @Override
            public DLNode next() {
                if (this.hasNext()) {
                    return this.output.next();
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();

            }
        }
    }

    public static UnionAll union_all(DLXPath xpath1, DLXPath xpath2) {
        return new UnionAll(new DLXPath[]{xpath1, xpath2});
    }

    public static UnionAll union_all(DLXPath... xpaths) {
        return new UnionAll(xpaths);
    }

//        public static void junk(){
//xpath(
//	iff(
//		xpath(root("DataLayerSlice"),child("ListOfMasterCase"),child("*"),child("ListOfHousehold"),child("*"),child("ListOfHouseholdPerson"),child("*"),child("ListOfPerson"),child("Person"),filter(isEqual(xpath(child("Id")),xpath(var("ParentEntity"),child("BaseEntity"),child("Contact_ID"))))),
//                xpath(root("DataLayerSlice"),child("ListOfMasterCase"),child("*"),child("ListOfHousehold"),child("*"),child("ListOfHouseholdPerson"),child("*"),child("ListOfPerson"),child("Person"),filter(isEqual(xpath(child("Id")),xpath(var("ParentEntity"),child("BaseEntity"),child("Contact_ID"))))),
//		xpath(
//			spawn_child_entity(
//				"ListOfPerson", 
//				"Person",
//				"Person",
//				xpath(
//					bcLookup(
//						"Contact",
//						"Id",
//						xpath(var("ParentEntity"),child("BaseEntity"),child("Contact_ID"))
//					)
//				)
//			)
//		)
//	)
//);
//          //  xpath(root("DataLayerSlice"),child("ListOfMasterCase"),child("*"),child("ListOfHousehold"),child("*"),child("ListOfHouseholdPerson"),child("*"),child("ListOfPerson"),child("Person"),filter(isEqual(xpath(child("Id")),xpath(var("ParentEntity"),child("BaseEntity"),child("Contact_ID")))))
//        }
    // /////////////////////////
    // /////////////////////////
    public static class SwitchOp extends XPathOp {

        DLXPath testXPath, defaultXPath;
        DLXPath[] inputXPath;

        public SwitchOp(Object... inputXPath) {
            if (inputXPath.length == 0) {
                throw new IllegalArgumentException("Please input a test xpath to match with!");
            }

            // If we have a default output argument, detect it, don't store it in our inputXPath array
            if (inputXPath.length % 2 == 0) {
                this.inputXPath = new DLXPath[inputXPath.length - 2];
            } else {
                this.inputXPath = new DLXPath[inputXPath.length - 1];
            }

            DLXPath transformedXPath;
            int ctr = 0;
            for (Object xpath : inputXPath) {
                if (xpath instanceof String) {
                    transformedXPath = new DLXPath(new Value((String) xpath));
                } else if (xpath instanceof DLXPath) {
                    transformedXPath = (DLXPath) xpath;
                } else {
                    throw new IllegalArgumentException("Make sure that your arguments are either xpath or String.");
                }
                // If we are processing our default id, store it in the default id variable, instead of the array
                if (ctr == inputXPath.length - 1 && inputXPath.length % 2 == 0) {
                    this.defaultXPath = transformedXPath;
                } else if (ctr == 0) {
                    this.testXPath = transformedXPath;
                } else {
                    this.inputXPath[ctr - 1] = transformedXPath;
                }

                this.addChild(transformedXPath);
                ctr++;
            }
        }

        @Override
        public String toString() {
            DLXPath[] strXPathArray;
            if (defaultXPath != null) {
                strXPathArray = new DLXPath[inputXPath.length + 2];
            } else {
                strXPathArray = new DLXPath[inputXPath.length + 1];
            }

            strXPathArray[0] = testXPath;

            int i = 1;
            for (DLXPath xpath : inputXPath) {
                strXPathArray[i] = xpath;
                i++;
            }

            if (defaultXPath != null) {
                strXPathArray[i] = defaultXPath;
            }

            return "iff(" + StringUtils.join(strXPathArray, ",") + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new SwitchOpIterator(input, context);
        }

        // so for every input, this will run the test and
        public class SwitchOpIterator implements Iterator<DLNode> {

            DLContext context;
            Iterator<DLNode> input;
            Iterator<DLNode> output;

            public SwitchOpIterator(Iterator<DLNode> input, DLContext context) {
                this.input = input;
                this.context = context;
            }

            @Override
            public boolean hasNext() {
                for (;;) {
                    if (output != null && output.hasNext()) {
                        return true;
                    }
                    if (input.hasNext()) {
                        DLNode curInput = input.next();
                        int inputLength = SwitchOp.this.inputXPath.length;
                        int ctr = 0;
                        boolean matchFound = false;
                        DLXPath defaultOutputXPath = SwitchOp.this.defaultXPath;

                        while (ctr < inputLength - 1 && !matchFound) {
                            DLXPath matchingXPath = SwitchOp.this.inputXPath[ctr];
                            DLXPath outputXPath = SwitchOp.this.inputXPath[ctr + 1];
                            DLXPath currentXPath = xpath(filter(isEqual(SwitchOp.this.testXPath, matchingXPath)));
                            logger.debug("testXPath " + ctr + ": " + SwitchOp.this.testXPath.toStringTree());
                            logger.debug("matchingXPath " + ctr + ": " + matchingXPath.toStringTree());
                            logger.debug("outputXPath " + ctr + ": " + outputXPath.toStringTree());
                            logger.debug("currentXPath " + ctr + ": " + currentXPath.toStringTree());
                            Iterator<DLNode> testIter = curInput.selectIterator(context, currentXPath);
                            if (testIter.hasNext()) {
                                this.output = curInput.selectIterator(context, outputXPath);
                                matchFound = true;
                            } else {
                                if (defaultOutputXPath != null) {
                                    this.output = curInput.selectIterator(context, defaultOutputXPath);
                                }
                            }
                            ctr = ctr + 2;
                            logger.debug("matchFound: " + matchFound);
                        }

                        if ((defaultOutputXPath != null) && inputLength == 0) {
                            this.output = curInput.selectIterator(context, defaultOutputXPath);
                        }
                        continue;
                    }
                    return false;
                }
            }

            @Override
            public DLNode next() {
                if (this.hasNext()) {
                    return this.output.next();
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();

            }
        }
    }

    public static SwitchOp switchxp(Object... inputs) {
        return new SwitchOp(inputs);
    }

    public static class IffOp extends XPathOp {

        DLXPath testXPath;
        DLXPath trueXPath;
        DLXPath falseXPath;

        public IffOp(DLXPath testXPath, DLXPath trueXPath, DLXPath falseXPath) {
            this.testXPath = testXPath;
            this.trueXPath = trueXPath;
            this.falseXPath = falseXPath;
            this.addChild(testXPath);
            this.addChild(trueXPath);
            this.addChild(falseXPath);
        }

        @Override
        public String toString() {
            return "iff(" + StringUtils.join(new DLXPath[]{testXPath, trueXPath, falseXPath}, ",") + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new IffOpIterator(input, context);
        }

        // so for every input, this will run the test and
        public class IffOpIterator implements Iterator<DLNode> {

            DLContext context;
            Iterator<DLNode> input;
            Iterator<DLNode> output;

            public IffOpIterator(Iterator<DLNode> input, DLContext context) {
                this.input = input;
                this.context = context;
            }

            @Override
            public boolean hasNext() {
                for (;;) {
                    if (output != null && output.hasNext()) {
                        return true;
                    }
                    if (input.hasNext()) {
                        DLNode curInput = input.next();
                        Iterator<DLNode> testIter = curInput.selectIterator(context, IffOp.this.testXPath);
                        if (testIter.hasNext()) {
                            this.output = curInput.selectIterator(context, IffOp.this.trueXPath);
                        } else {
                            this.output = curInput.selectIterator(context, IffOp.this.falseXPath);
                        }
                        continue;
                    }
                    return false;
                }
            }

            @Override
            public DLNode next() {
                if (this.hasNext()) {
                    return this.output.next();
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();

            }
        }
    }

    public static IffOp iff(DLXPath testXPath, DLXPath trueXPath, DLXPath falseXPath) {
        return new IffOp(testXPath, trueXPath, falseXPath);
    }

    public static IffOp iff(DLXPath testXPath, String trueValue, String falseValue) {
        return new IffOp(testXPath, new DLXPath(new Value(trueValue)), new DLXPath(new Value(falseValue)));
    }

    public static IffOp iff(DLXPath testXPath, DLXPath trueXPath, String falseValue) {
        return new IffOp(testXPath, trueXPath, new DLXPath(new Value(falseValue)));
    }

    public static IffOp iff(DLXPath testXPath, String trueValue, DLXPath falseXPath) {
        return new IffOp(testXPath, new DLXPath(new Value(trueValue)), falseXPath);
    }
    // /////////////////////////

    /**
     * see if the current node and expression result in more than 1 node. if so
     * true, else false;
     *
     * http://msdn.microsoft.com/en-us/library/ms256122(v=vs.110).aspx
     *
     * @author Josh Monzon
     */
    public static class Filter extends XPathOp {

        DLXPath xpath;

        public Filter(DLXPath xpath) {
            this.xpath = xpath;
            this.addChild(this.xpath);
        }

        @Override
        public String toString() {
            return "filter(" + xpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new FilterIterator(input, context, this.xpath);
        }

        public static class FilterIterator extends XPathImmediateChildIter {

            public DLXPath xpath;

            public FilterIterator(Iterator<DLNode> input, DLContext context, DLXPath xpath) {
                super(input, context);
                this.xpath = xpath;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                // logger.debug("Test filter current node="+contextNode.toXmlString());
                if (contextNode.selectIterator(context, xpath).hasNext()) {
                    List<DLNode> list = new ArrayList<DLNode>();
                    list.add(contextNode);
                    // logger.debug("SUCCESS:Test filter current node="+contextNode.toXmlString());
                    return list;
                }
                // logger.debug("FAIL:Test filter current node="+contextNode.toXmlString());
                return Collections.emptyList();
            }
        }
    }

    public static Filter filter(DLXPath xpath) {
        return new Filter(xpath);
    }

    public static BooleanFilter filter(BooleanOp booleanOp) {
        return new BooleanFilter(booleanOp);
    }

    /**
     * This preforms the boolean filter on its boolean operators.
     *
     * @author Josh Monzon
     *
     */
    public static class BooleanFilter extends XPathOp {

        BooleanOp booleanOp;

        public BooleanFilter(BooleanOp booleanOp) {
            this.booleanOp = booleanOp;
            this.addChild(this.booleanOp);
        }

        @Override
        public String toString() {
            return "filter(" + booleanOp + ")";
        }

        @Override
        public String toStringTree() {
            return "filter(" + booleanOp + ")";
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new BooleanFilterIterator(input, context, this.booleanOp);
        }

        public static class BooleanFilterIterator extends XPathImmediateChildIter {

            public BooleanOp booleanOp;

            public BooleanFilterIterator(Iterator<DLNode> input, DLContext context, BooleanOp booleanOp) {
                super(input, context);
                this.booleanOp = booleanOp;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                boolean result = booleanOp.evaluate(contextNode, context);
                // logger.debug("Test filter current node="+contextNode.toXmlString());
                if (result) {
                    List<DLNode> list = new ArrayList<DLNode>();
                    list.add(contextNode);
                    // logger.debug("SUCCESS:Test filter current node="+contextNode.toXmlString());
                    return list;
                }
                // logger.debug("FAIL:Test filter current node="+contextNode.toXmlString());
                return Collections.emptyList();
            }
        }
    }

    /**
     * This is a fake operator I made up to help work with xpath comparsions
     * like '/author[last_name='parks']. We will use this Value operator for
     * 'parks' and treat both sides of equals as xpath.
     *
     * @author Josh Monzon
     */
    public static class Value extends XPathOp {

        public String value;

        public Value(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "value(" + value + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ValueIterator(input, context, value);
        }

        public static class ValueIterator extends XPathImmediateChildIter {

            String value;

            public ValueIterator(Iterator<DLNode> input, DLContext context, String value) {
                super(input, context);
                this.value = value;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>(1);
                list.add(new DLValue(value));
                return list;
            }
        }
    }

    public static Value value(String value) {
        return new Value(value);
    }

    /**
     * Generates a random Id which will NOT be static for every time you fire
     * this xpath. New id generated per xpath that uses the method.
     */
    public static class RandomId extends XPathOp {

        @Override
        public String toString() {
            return "randomId()";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new RandomIterator(input, context);
        }

        public static class RandomIterator extends XPathImmediateChildIter {

            public RandomIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);

            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList(1);
                list.add(new DLValue(UUID.randomUUID().toString().replace("-", "")));
                return list;
            }
        }
    }

    public static RandomId randomId() {
        return new RandomId();
    }

    /**
     * Generates a random Id which will be static for every time you fire this
     * xpath. New id generated per xpath that uses the method.
     */
    public static class StaticRandomId extends XPathOp {

        public String id;

        public StaticRandomId() {
            this.id = UUID.randomUUID().toString().replace("-", "");
        }

        @Override
        public String toString() {
            return "staticRandomId()";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new StaticRandomIterator(input, context, id);
        }

        public static class StaticRandomIterator extends XPathImmediateChildIter {

            String id;

            public StaticRandomIterator(Iterator<DLNode> input, DLContext context, String id) {
                super(input, context);
                this.id = id;
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList(1);
                list.add(new DLValue(this.id));
                return list;
            }
        }
    }

    public static StaticRandomId staticRandomId() {
        return new StaticRandomId();
    }

    public static class InOp extends MultiBooleanComparisonOp {

        public InOp(DLXPath leftXPath, DLXPath... rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean booleanCompare(String leftValue, String rightValue) {
            return leftValue.equals(rightValue);
        }

        @Override
        public String toString() {
            return "isIn(" + this.leftXPath.toString() + "," + StringUtility.joinStrings(",", Lists.newArrayList(rightXPaths)) + ")";
        }
    }

    public static InOp isIn(DLXPath... leftRightXPaths) {
        DLXPath[] rightXPaths = new DLXPath[leftRightXPaths.length - 1];
        for (int i = 1; i < leftRightXPaths.length; i++) {
            rightXPaths[i - 1] = new DLXPath(leftRightXPaths[i]);
        }

        return new InOp(leftRightXPaths[0], rightXPaths);
    }

//	public static InOp isIn(DLXPath leftXPath, DLXPath... rightXPath) {
//		return new InOp(leftXPath, rightXPath);
//	}
    public static InOp isIn(DLXPath leftXPath, String... rightXPath) {
        DLXPath[] rightXPaths = new DLXPath[rightXPath.length];
        for (int i = 0; i < rightXPath.length; i++) {
            Object obj = rightXPath[i];

            rightXPaths[i] = new DLXPath(new Value((String) obj));

        }
        return new InOp(leftXPath, rightXPaths);
    }

    public static InOp isIn(String value, DLXPath... rightXPath) {
        return new InOp(new DLXPath(new Value(value)), rightXPath);
    }

    public static InOp isIn(String leftString, String... rightString) {
        return isIn(new DLXPath(new Value(leftString)), rightString);
    }

//     public static InOp isIn(DLXPath leftXpath, DLXPath... rightXPath) {
//        return new InOp(leftXpath, rightXPath);
//    }
    public static EqualsOp isEqual(DLXPath leftXPath, DLXPath rightXPath) {
        return new EqualsOp(leftXPath, rightXPath);
    }

    public static EqualsOp isEqual(DLXPath leftXPath, String value) {
        return new EqualsOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static EqualsOp isEqual(String leftString, String rightString) {
        return new EqualsOp(new DLXPath(new Value(leftString)), new DLXPath(new Value(rightString)));
    }

    public static EqualsEntityOp isEntityEqual(String value, DLXPath rightXPath) {
        return new EqualsEntityOp(new DLXPath(new Value(value)), rightXPath);
    }

    public static EqualsEntityOp isEntityEqual(DLXPath leftXPath, DLXPath rightXPath) {
        return new EqualsEntityOp(leftXPath, rightXPath);
    }

    public static EqualsEntityOp isEntityEqual(DLXPath leftXPath, String value) {
        return new EqualsEntityOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static EqualsEntityOp isEntityEqual(String leftString, String rightString) {
        return new EqualsEntityOp(new DLXPath(new Value(leftString)), new DLXPath(new Value(rightString)));
    }

    public static NotEqualsEntityOp notEntityEqual(String value, DLXPath rightXPath) {
        return new NotEqualsEntityOp(new DLXPath(new Value(value)), rightXPath);
    }

    public static NotEqualsEntityOp notEntityEqual(DLXPath leftXPath, DLXPath rightXPath) {
        return new NotEqualsEntityOp(leftXPath, rightXPath);
    }

    public static NotEqualsEntityOp notEntityEqual(DLXPath leftXPath, String value) {
        return new NotEqualsEntityOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static NotEqualsEntityOp notEntityEqual(String leftString, String rightString) {
        return new NotEqualsEntityOp(new DLXPath(new Value(leftString)), new DLXPath(new Value(rightString)));
    }

    public static EqualsOp isEqual(String value, DLXPath rightXPath) {
        return new EqualsOp(new DLXPath(new Value(value)), rightXPath);
    }

    public static NotEqualsOp notEqual(DLXPath leftXPath, DLXPath rightXPath) {
        return new NotEqualsOp(leftXPath, rightXPath);
    }

    public static NotEqualsOp notEqual(DLXPath leftXPath, String value) {
        return new NotEqualsOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static NotEqualsOp notEqual(String value, DLXPath rightXPath) {
        return new NotEqualsOp(new DLXPath(new Value(value)), rightXPath);
    }

    // /bookstore/books/*[title=yo or title=ho]
    public static class OrOp extends BooleanOp {

        DLXPath[] xpaths;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("or(");
            for (int i = 0; i < xpaths.length; i++) {
                DLXPath xp = this.xpaths[i];
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(xp.toString());
            }
            sb.append(")");
            return sb.toString();
        }

        public OrOp(DLXPath... xpaths) {
            super();
            this.xpaths = xpaths;
            for (DLXPath xpath : xpaths) {
                this.addChildren(xpath);
            }
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            for (DLXPath xpath : xpaths) {
                if (contextNode.selectIterator(context, xpath).hasNext()) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class BooleanOrOp extends BooleanOp {

        BooleanOp[] ops;

        public BooleanOrOp(BooleanOp... ops) {
            super();
            this.ops = ops;
            for (BooleanOp op : ops) {
                this.addChildren(op);
            }
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            for (BooleanOp op : ops) {
                boolean result = op.evaluate(contextNode, context);
                if (result) {
                    return true;
                }
            }
            return false;
        }
    }

    public static OrOp isTrue(DLXPath xpath) {
        DLXPath[] xpaths = new DLXPath[]{xpath};
        return new OrOp(xpaths);
    }

    public static OrOp or(DLXPath... xpaths) {
        return new OrOp(xpaths);
    }

    public static BooleanOrOp or(DLXPath xpathLeft, BooleanOp rightBooleanOp) {
        BooleanOp[] ops = new BooleanOp[]{or(xpathLeft), rightBooleanOp};
        return new BooleanOrOp(ops);
    }

    public static BooleanOrOp or(BooleanOp leftBooleanOp, DLXPath rightXPath) {
        BooleanOp[] ops = new BooleanOp[]{leftBooleanOp, or(rightXPath)};
        return new BooleanOrOp(ops);
    }

    public static BooleanOrOp or(BooleanOp... ops) {
        return new BooleanOrOp(ops);
    }

    public static class AndOp extends BooleanOp {

        DLXPath[] xpaths;

        public AndOp(DLXPath... xpaths) {
            super();
            this.xpaths = xpaths;
            for (DLXPath xpath : xpaths) {
                this.addChildren(xpath);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("and(");
            for (int i = 0; i < xpaths.length; i++) {
                DLXPath xp = this.xpaths[i];
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(xp.toString());
            }
            sb.append(")");
            return sb.toString();
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            for (DLXPath xpath : xpaths) {
                if (!contextNode.selectIterator(context, xpath).hasNext()) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class BooleanAndOp extends BooleanOp {

        BooleanOp[] ops;

        public BooleanAndOp(BooleanOp... ops) {
            super();
            this.ops = ops;
            for (BooleanOp op : ops) {
                this.addChildren(op);
            }
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            for (BooleanOp op : ops) {
                boolean result = op.evaluate(contextNode, context);
                if (!result) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return "and(" + StringUtility.joinStrings(",", Lists.newArrayList(ops)) + ")";
        }
    }

    public static AndOp and(DLXPath... xpaths) {
        return new AndOp(xpaths);
    }

    public static BooleanAndOp and(BooleanOp... ops) {
        return new BooleanAndOp(ops);
    }

    public static abstract class BooleanDLNodeComparisonOp extends BooleanOp {

        DLXPath leftXPath;
        DLXPath rightXPath;

        public BooleanDLNodeComparisonOp(DLXPath leftXPath, DLXPath rightXPath) {
            super();
            if (leftXPath == null) {
                throw new IllegalArgumentException("leftXPath cannot be null");
            }
            if (rightXPath == null) {
                throw new IllegalArgumentException("rightXPath cannot be null");
            }
            this.leftXPath = leftXPath;
            this.rightXPath = rightXPath;
            this.addChildren(leftXPath, rightXPath);
        }

        public OgTree getLeft() {
            return (OgTree) this.getChild(0);
        }

        public OgTree getRight() {
            return (OgTree) this.getChild(1);
        }

        public abstract boolean booleanNodeCompare(DLNode leftValue, DLNode rightValue);

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            try {
                Iterator<DLNode> leftIter = contextNode.selectIterator(context, leftXPath);
                Iterator<DLNode> rightIter = contextNode.selectIterator(context, rightXPath);
                List<DLNode> leftValues = null;
                while (rightIter.hasNext()) {
                    DLNode rightNode = rightIter.next();
                    //String rightValue = rightNode.getValue();
                    if (rightNode != null) {
                        if (leftValues == null) {
                            leftValues = new ArrayList<DLNode>();
                            while (leftIter.hasNext()) {
                                DLNode leftNode = leftIter.next();
                                //String leftValue = leftNode.getValue();
                                if (leftNode != null) {
                                    leftValues.add(leftNode);
                                    if (this.booleanNodeCompare(leftNode, rightNode)) {
                                        return true;
                                    }
                                }
                            }
                        } else {
                            for (DLNode leftNode : leftValues) {
                                if (this.booleanNodeCompare(leftNode, rightNode)) {
                                    return true;
                                }
                            }
                        }
                    }
                }
                return false;
            } catch (Exception e) {
                throw new RuntimeException("Cannot process boolean xpath operator(" + this.toString() + ")", e);
            }
        }

    }

    public static abstract class BooleanComparisonOp extends BooleanOp {

        DLXPath leftXPath;
        DLXPath rightXPath;

        public BooleanComparisonOp(DLXPath leftXPath, DLXPath rightXPath) {
            super();
            if (leftXPath == null) {
                throw new IllegalArgumentException("leftXPath cannot be null");
            }
            if (rightXPath == null) {
                throw new IllegalArgumentException("rightXPath cannot be null");
            }
            this.leftXPath = leftXPath;
            this.rightXPath = rightXPath;
            this.addChildren(leftXPath, rightXPath);
        }

        public OgTree getLeft() {
            return (OgTree) this.getChild(0);
        }

        public OgTree getRight() {
            return (OgTree) this.getChild(1);
        }

        public abstract boolean booleanCompare(String leftValue, String rightValue);

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            try {
                Iterator<DLNode> leftIter = contextNode.selectIterator(context, leftXPath);
                Iterator<DLNode> rightIter = contextNode.selectIterator(context, rightXPath);
                List<String> leftValues = null;
                while (rightIter.hasNext()) {
                    DLNode rightNode = rightIter.next();
                    String rightValue = rightNode.getValue();
                    if (rightValue != null) {
                        if (leftValues == null) {
                            leftValues = new ArrayList<String>();
                            while (leftIter.hasNext()) {
                                DLNode leftNode = leftIter.next();
                                String leftValue = leftNode.getValue();
                                if (leftValue != null) {
                                    leftValues.add(leftValue);
                                    if (this.booleanCompare(leftValue, rightValue)) {
                                        return true;
                                    }
                                }
                            }
                        } else {
                            for (String leftValue : leftValues) {
                                if (this.booleanCompare(leftValue, rightValue)) {
                                    return true;
                                }
                            }
                        }
                    }
                }
                return false;
            } catch (Exception e) {
                throw new RuntimeException("Cannot process boolean xpath operator(" + this.toString() + ")", e);
            }
        }

    }

    //same as oring together booleanOp(left,right1) for each right
    public static abstract class MultiBooleanComparisonOp extends BooleanOp {

        DLXPath leftXPath;
        DLXPath[] rightXPaths;

        public MultiBooleanComparisonOp(DLXPath leftXPath, DLXPath... rightXPaths) {
            super();
            if (leftXPath == null) {
                throw new IllegalArgumentException("leftXPath cannot be null");
            }
            if (rightXPaths == null) {
                throw new IllegalArgumentException("rightXPath cannot be null");
            }
            this.leftXPath = leftXPath;
            this.rightXPaths = rightXPaths;
            this.addChildren(leftXPath);
            for (DLXPath rightXPath : rightXPaths) {
                this.addChildren(rightXPath);
            }
        }

        public abstract boolean booleanCompare(String leftValue, String rightValue);

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            try {
                for (int i = 0; i < rightXPaths.length; i++) {
                    Iterator<DLNode> leftIter = contextNode.selectIterator(context, leftXPath);
                    //UnionAll.UnionAllIterator rightIter = new UnionAll.UnionAllIterator(Lists.newArrayList(contextNode).iterator(), context, rightXPaths);
                    Iterator<DLNode> rightIter = contextNode.selectIterator(context, rightXPaths[i]);
                    List<String> leftValues = null;
                    while (rightIter.hasNext()) {
                        DLNode rightNode = rightIter.next();
                        String rightValue = rightNode.getValue();
                        if (rightValue != null) {
                            if (leftValues == null) {
                                leftValues = new ArrayList<String>();
                                while (leftIter.hasNext()) {
                                    DLNode leftNode = leftIter.next();
                                    String leftValue = leftNode.getValue();
                                    if (leftValue != null) {
                                        leftValues.add(leftValue);
                                        if (this.booleanCompare(leftValue, rightValue)) {
                                            return true;
                                        }
                                    }
                                }
                            } else {
                                for (String leftValue : leftValues) {
                                    if (this.booleanCompare(leftValue, rightValue)) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
                return false;
            } catch (Exception e) {
                throw new RuntimeException("Cannot process boolean xpath operator(" + this.toString() + ")", e);
            }
        }

    }

    // attempts to infer data type based on data content and does the proper
    // comparison
    public static abstract class FlexibleBooleanComparisonOp extends BooleanComparisonOp {

        public FlexibleBooleanComparisonOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        public abstract boolean compareDates(Date left, Date right);

        public abstract boolean compareBigDecimals(BigDecimal left, BigDecimal right);

        public abstract boolean compareStrings(String left, String right);

        @Override
        public boolean booleanCompare(String leftValue, String rightValue) {
            Date leftDate = getDateOrNull(leftValue);
            if (leftDate != null) {
                Date rightDate = getDateOrNull(rightValue);
                if (rightDate != null) {
                    return this.compareDates(leftDate, rightDate);
                }
            }
            BigDecimal leftBigDecimal = getBigDecimalOrNull(leftValue);
            if (leftBigDecimal != null) {
                BigDecimal rightBigDecimal = getBigDecimalOrNull(rightValue);
                if (rightBigDecimal != null) {
                    return this.compareBigDecimals(leftBigDecimal, rightBigDecimal);
                }
            }
            return this.compareStrings(leftValue, rightValue);
        }
    }

    // Greater than
    public static class GreaterThanOp extends FlexibleBooleanComparisonOp {

        public GreaterThanOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean compareDates(Date left, Date right) {
            return left.compareTo(right) > 0;
        }

        @Override
        public boolean compareBigDecimals(BigDecimal left, BigDecimal right) {
            return left.compareTo(right) > 0;
        }

        @Override
        public boolean compareStrings(String left, String right) {
            return left.compareTo(right) > 0;
        }
    }

    public static GreaterThanOp greaterThan(DLXPath leftXPath, DLXPath rightXPath) {
        return new GreaterThanOp(leftXPath, rightXPath);
    }

    public static GreaterThanOp greaterThan(DLXPath leftXPath, String value) {
        return new GreaterThanOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static GreaterThanOp greaterThan(String value, DLXPath rightXPath) {
        return new GreaterThanOp(new DLXPath(new Value(value)), rightXPath);
    }

    // Greater than or equal
    public static class GreaterThanOrEqualToOp extends FlexibleBooleanComparisonOp {

        public GreaterThanOrEqualToOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean compareDates(Date left, Date right) {
            return left.compareTo(right) >= 0;
        }

        @Override
        public boolean compareBigDecimals(BigDecimal left, BigDecimal right) {
            return left.compareTo(right) >= 0;
        }

        @Override
        public boolean compareStrings(String left, String right) {
            return left.compareTo(right) >= 0;
        }
    }

    public static GreaterThanOrEqualToOp greaterThanOrEqualTo(DLXPath leftXPath, DLXPath rightXPath) {
        return new GreaterThanOrEqualToOp(leftXPath, rightXPath);
    }

    public static GreaterThanOrEqualToOp greaterThanOrEqualTo(DLXPath leftXPath, String value) {
        return new GreaterThanOrEqualToOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static GreaterThanOrEqualToOp greaterThanOrEqualTo(String value, DLXPath rightXPath) {
        return new GreaterThanOrEqualToOp(new DLXPath(new Value(value)), rightXPath);
    }

    // Less than
    public static class LessThanOp extends FlexibleBooleanComparisonOp {

        public LessThanOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean compareDates(Date left, Date right) {
            return left.compareTo(right) < 0;
        }

        @Override
        public boolean compareBigDecimals(BigDecimal left, BigDecimal right) {
            return left.compareTo(right) < 0;
        }

        @Override
        public boolean compareStrings(String left, String right) {
            return left.compareTo(right) < 0;
        }
    }

    public static LessThanOp lessThan(DLXPath leftXPath, DLXPath rightXPath) {
        return new LessThanOp(leftXPath, rightXPath);
    }

    public static LessThanOp lessThan(DLXPath leftXPath, String value) {
        return new LessThanOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static LessThanOp lessThan(String value, DLXPath rightXPath) {
        return new LessThanOp(new DLXPath(new Value(value)), rightXPath);
    }

    // Less than or equal
    public static class LessThanOrEqualToOp extends FlexibleBooleanComparisonOp {

        public LessThanOrEqualToOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean compareDates(Date left, Date right) {
            return left.compareTo(right) <= 0;
        }

        @Override
        public boolean compareBigDecimals(BigDecimal left, BigDecimal right) {
            return left.compareTo(right) <= 0;
        }

        @Override
        public boolean compareStrings(String left, String right) {
            return left.compareTo(right) <= 0;
        }
    }

    public static LessThanOrEqualToOp lessThanOrEqualTo(DLXPath leftXPath, DLXPath rightXPath) {
        return new LessThanOrEqualToOp(leftXPath, rightXPath);
    }

    public static LessThanOrEqualToOp lessThanOrEqualTo(DLXPath leftXPath, String value) {
        return new LessThanOrEqualToOp(leftXPath, new DLXPath(new Value(value)));
    }

    public static LessThanOrEqualToOp lessThanOrEqualTo(String value, DLXPath rightXPath) {
        return new LessThanOrEqualToOp(new DLXPath(new Value(value)), rightXPath);
    }

    public static Date getDateOrNull(String value) {
        if (value == null) {
            return null;
        }
        // 0123456789012345678
        // 2014-02-24 09:31:40

        if (value.length() == 10 && value.charAt(4) == '-' && value.charAt(7) == '-') {
            value = value + " 00:00:00";
        }
        if (value.length() != 19) {
            return null;
        }
        if (value.charAt(4) != '-' || value.charAt(7) != '-' || value.charAt(10) != ' ' || value.charAt(13) != ':' || value.charAt(16) != ':') {
            return null;
        }
        try {
            Date date = com.armedica.onegate.datalayer.util.DateUtils.parse_yyyy_mm_dd_hh_mm_ss(value);
            return date;
        } catch (Exception e) {
            return null;
        }

    }

    public static String dateToString(Date value) {
        return DateUtils.format_yyyy_mm_dd_hh_mm_ss(value);
    }

    public static BigDecimal getBigDecimalOrNull(String value) {
        if (value == null) {
            return null;
        }
        BigDecimal bigDecimal = StringUtility.toBigDecimal(value, null);
        return bigDecimal;
    }

    /**
     * Performs equality.
     *
     * Examples from:
     * http://msdn.microsoft.com/en-us/library/ms256135(v=vs.110).aspx
     *
     * Example 1: author[last-name = "Bob"] All <author> elements that contain
     * at least one <last-name> element with the id Bob.
     *
     * So here the left id is an xpath as which might result in multiple nodes.
     * It takes the approach that only one author/last-name must be set to "Bob"
     * for this to be true.
     *
     * Example 2: author[last-name = /editor/last-name] All <author> elements
     * that contain a <last-name> element that is the same as the <last-name>
     * element inside the <editor> element under the root element.
     *
     * So here both sides are xpaths. For consistency with example 1, we'll
     * check all permutations of left and right paths for a match. So if there
     * are 2 last-name and 3 /editor/last-names we'll check 6 combos and only
     * one needs to match up for use to claim victory.
     *
     * Note that this class treat both the left and right as xpaths. We do this
     * by creating an xpath that returns a simple String like "Bob" so we don't
     * need to worry about it
     *
     * @author Josh Monzon
     *
     */
    public static class EqualsOp extends BooleanComparisonOp {

        public EqualsOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean booleanCompare(String leftValue, String rightValue) {
            return leftValue.equals(rightValue);
        }

        @Override
        public String toString() {
            return "isEqual(" + this.leftXPath.toString() + "," + this.rightXPath.toString() + ")";
        }
    }

    public static class EqualsEntityOp extends BooleanDLNodeComparisonOp {

        public EqualsEntityOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean booleanNodeCompare(DLNode leftValue, DLNode rightValue) {
//            DLNode leftNode = (DLEntity) leftValue.getPublicReference();
//            DLNode rightNode = (DLEntity) rightValue.getPublicReference();
//            return leftNode.equals(rightNode);
            int leftRefId = ((DLEntity) leftValue).getInnermostRefId();
            int rightRefId = ((DLEntity) rightValue).getInnermostRefId();
            boolean match = leftRefId == rightRefId;
            if (!match && leftValue instanceof SiebelBusCompEntity && rightValue instanceof SiebelBusCompEntity) {
                return ((SiebelBusCompEntity) leftValue).isEntityEqual((SiebelBusCompEntity) rightValue);
            } else {
                return match;
            }

        }

        @Override
        public String toString() {
            return "isEqual(" + this.leftXPath.toString() + "," + this.rightXPath.toString() + ")";
        }
    }

    public static class NotEqualsEntityOp extends BooleanDLNodeComparisonOp {

        public NotEqualsEntityOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean booleanNodeCompare(DLNode leftValue, DLNode rightValue) {
//            DLNode leftNode = (DLEntity) leftValue.getPublicReference();
//            DLNode rightNode = (DLEntity) rightValue.getPublicReference();
//            return !leftNode.equals(rightNode);

            int leftRefId = ((DLEntity) leftValue).getInnermostRefId();
            int rightRefId = ((DLEntity) rightValue).getInnermostRefId();
            return leftRefId != rightRefId;

        }

        @Override
        public String toString() {
            return "isEqual(" + this.leftXPath.toString() + "," + this.rightXPath.toString() + ")";
        }
    }

    public static class NotEqualsOp extends EqualsOp {

        public NotEqualsOp(DLXPath leftXPath, DLXPath rightXPath) {
            super(leftXPath, rightXPath);
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            return !super.evaluate(contextNode, context);
        }

        @Override
        public String toString() {
            return "notEqual(" + this.leftXPath.toString() + "," + this.rightXPath.toString() + ")";
        }
    }

    public static class BooleanNotOp extends BooleanOp {

        BooleanOp op;

        public BooleanNotOp(BooleanOp op) {
            super();
            this.op = op;
            this.addChildren(op);
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            boolean result = op.evaluate(contextNode, context);
            return !result;
        }

        @Override
        public String toString() {
            return "not(" + op + ")";
        }
    }

    public static BooleanNotOp not(BooleanOp op) {
        return new BooleanNotOp(op);
    }

    public static class NotBooleanOp extends BooleanOp {

        DLXPath leftXPath;

        public NotBooleanOp(DLXPath leftXPath) {
            super();
            this.leftXPath = leftXPath;
            this.addChildren(leftXPath);
        }

        public OgTree getLeft() {
            return (OgTree) this.getChild(0);
        }

        @Override
        public String toString() {
            return "not(" + this.leftXPath.toString() + ")";
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            Iterator<DLNode> leftIter = contextNode.selectIterator(context, leftXPath);
            if (leftIter.hasNext()) {
                return false;
            }
            return true;
        }
    }

    public static NotBooleanOp not(DLXPath leftXPath) {
        return new NotBooleanOp(leftXPath);
    }

    /**
     * Not empty mean you select a node with a id no equal to "".
     *
     * @author Josh Monzon
     */
    public static class NotEmptyOp extends BooleanOp {

        DLXPath leftXPath;

        public NotEmptyOp(DLXPath leftXPath) {
            super();
            this.leftXPath = leftXPath;
            this.addChildren(leftXPath);
        }

        public OgTree getLeft() {
            return (OgTree) this.getChild(0);
        }

        @Override
        public String toString() {
            return "valueNotEmpty(" + this.leftXPath.toString() + ")";
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            Iterator<DLNode> leftIter = contextNode.selectIterator(context, leftXPath);
            if (leftIter.hasNext()) {
                DLNode node = leftIter.next();
                String value = node.getValue();
                boolean notEmpty = value != null && !value.isEmpty();
                return notEmpty;
            }
            return false;
        }
    }

    public static NotEmptyOp valueNotEmpty(DLXPath leftXPath) {
        return new NotEmptyOp(leftXPath);
    }

    /**
     * Is empty means you select either no nodes, or the id you are selecting is
     * ""
     *
     * @author Josh Monzon
     */
    public static class IsEmptyOp extends NotEmptyOp {

        public IsEmptyOp(DLXPath leftXPath) {
            super(leftXPath);
        }

        @Override
        public String toString() {
            return "valueIsEmpty(" + this.leftXPath.toString() + ")";
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            return !super.evaluate(contextNode, context);
        }
    }

    public static IsEmptyOp valueIsEmpty(DLXPath leftXPath) {
        return new IsEmptyOp(leftXPath);
    }

    // plumb all xpaths with an object, that object can take named node refs.
    // One obvious one will be ROOT so we can use it to evaluate '/blah'. Also
    // we can use any other named node. Since our predicates treat literals
    // as nodes, these can be node refs and we now have binding for free!
    public void blah() {
        DLXPath xpath = new DLXPath();
        // xpath.iterator(null /*options*/);
        // xpath passes the options down to the operators
        // operators pass them down further if needed.

    }

    public static class DLContext {

        public static Logger logger = Logger.getLogger(DLContext.class);
        private boolean viewAll = false;
        // when running inside CCE this is set to the rule that is executing
        public ChangeCaptureEngine.RuleContext ruleContext = null;

        /**
         * This is the slice that is in context. Dynamic data belongs to a
         * slice.
         */
        Slice dataLayerSlice;

        public void setViewAll(boolean viewAll) {
            this.viewAll = viewAll;
        }

        public boolean viewAll() {
            return viewAll;
        }

        public Configuration getConfiguration() {
            if (this.dataLayerSlice != null) {
                DataLayer dataLayer = this.dataLayerSlice.getDataLayer();
                if (dataLayer != null) {
                    return dataLayer.configuration;
                }
            }
            return null;
        }

        public void setConfiguration(Configuration value) {
            //this.configuration = id;
        }

        boolean lazyFieldTracking = true;

        /**
         * When set to true, lazy links and fields will automatically be pulled
         * into existence. When set to false, you will only see what is already
         * in memory.
         */
        boolean autoPull = true;

        public DLContext autoPull() {
            this.autoPull = true;
            return this;
        }

        public DLContext noAutoPull() {
            this.autoPull = false;
            return this;
        }

        public DLContext lazyFieldTracking() {
            this.lazyFieldTracking = true;
            return this;
        }

        public DLContext noLazyFieldTracking() {
            this.lazyFieldTracking = false;
            return this;
        }

        public boolean isLazyFieldTracking() {
            return this.lazyFieldTracking;
        }

        // Reference to root for evaluating expressions starting with '/'
        // We default this by chasing up parent links to the top element.
        DLNode rootNode;

        // Current node is the node that the XPath processor is looking at when
        // it
        // begins evaluation of a query. In other words, the current node is the
        // first
        // context node that the XPath processor uses when it starts to execute
        // the query.
        // During evaluation of a query, the current node does not change. If
        // you pass a
        // document to the XPath processor, the root node is the current node.
        // If you pass
        // a node to the XPath processor, that node is the current node.
        //
        // Because current node sounds a lot like context node, I am just going
        // to call this
        // the starting node. It is not clear to me how to access this node in
        // xpath but we
        // might as well track it.
        DLNode startingNode;

        // A context node is the node the XPath processor is currently looking
        // at. The context
        // node changes as the XPath processor evaluates a query. If you pass a
        // document to the
        // XPath processor, the root node is the initial context node. If you
        // pass a node to
        // the XPath processor, the node that you pass is the initial context
        // node. During
        // evaluation of a query, the initial context node is also the current
        // node (AKA starting node).
        // DLNode contextNode;
        // The position of the context node in the document order, relative to
        // its siblings
        // int position=1;
        // The size of the context that is, the number of siblings of the
        // context node plus one
        // ?
        // Variables allow an xpath to reference other node lists. These can be
        // related nodes
        // from the same tree, unrelatated nodes from a different tree, or even
        // manufactured
        // nodes. All variables are node lists even if they are a scalar (list
        // of 1).
        Map<String, List<DLNode>> variables = new HashMap<String, List<DLNode>>();

        public void SetRoot(DLNode contextNode) {
            this.startingNode = contextNode;
            this.rootNode = contextNode;
            int i = 0;
            while (this.rootNode.getParentNode() != null) {
                if (i++ > 10000) {
                    throw new RuntimeException("Looks like contextNode [" + contextNode.getTagName() + "] has circular parent links");
                }
                this.rootNode = this.rootNode.getParentNode();

            }
        }

        public DLContext() {

        }

        public DLContext(DLNode contextNode) {
            this.SetRoot(contextNode);
        }

        @Override
        public DLContext clone() {
            DLContext clone = new DLContext(this.startingNode);
            clone.autoPull = this.autoPull;
            clone.dataLayerSlice = this.dataLayerSlice;
            for (Map.Entry<String, List<DLNode>> elem : this.variables.entrySet()) {
                clone.variables.put(elem.getKey(), new ArrayList<DLNode>(elem.getValue()));
            }
            return clone;
        }

        public void setVariable(String name, List<DLNode> value) {
            this.variables.put(name, value);
        }

        public void setVariable(String name, DLNode value) {
            List<DLNode> list = new ArrayList<DLNode>(1);
            list.add(value);
            this.setVariable(name, list);
        }

        public void setVariable(String name, String value) {
            DLValue valueNode = new DLValue(value);
            this.setVariable(name, valueNode);
        }

        public List<DLNode> getVariable(String name) {
            List<DLNode> value = this.variables.get(name);
            if (value == null) {
                value = Collections.emptyList();
            }
            return value;
        }

        public Slice getDataLayerSlice() {
            return this.dataLayerSlice;
        }

        public void setDataLayerSlice(Slice value) {
            this.dataLayerSlice = value;
        }

    }

    public static class MaxDate extends XPathOp {

        DLXPath xpath;

        public MaxDate(DLXPath xpath) {
            this.xpath = xpath;
            this.addChild(this.xpath);
        }

        @Override
        public String toString() {
            return "maxDate(" + xpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new MaxDateIterator(input, context, this.xpath);
        }

        public static class MaxDateIterator extends XPathSetIter {

            public DLXPath xpath;

            public MaxDateIterator(Iterator<DLNode> input, DLContext context, DLXPath xpath) {
                super(input, context);
                this.xpath = xpath;
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> outputSet = new ArrayList<DLNode>();
                Date curMaxDate = null;
                for (DLNode inputNode : inputSet) {
                    // Make sure each input is only added one time
                    boolean addedInput = false;
                    for (DLNode node : inputNode.select(context, xpath)) {
                        String value = node.getValue();
                        if (value != null) {
                            // 2014-02-24 09:31:40
                            Date date = com.armedica.onegate.datalayer.util.DateUtils.parse_yyyy_mm_dd_hh_mm_ss(value);
                            if (curMaxDate == null) {
                                curMaxDate = date;
                                if (!addedInput) {
                                    outputSet.add(inputNode);
                                    addedInput = true;
                                }
                            } else if (curMaxDate.equals(date)) {
                                curMaxDate = date;
                                if (!addedInput) {
                                    outputSet.add(inputNode);
                                    addedInput = true;
                                }
                            } else if (curMaxDate.before(date)) {
                                curMaxDate = date;
                                outputSet.clear();
                                if (!addedInput) {
                                    outputSet.add(inputNode);
                                    addedInput = true;
                                }
                            }
                        }
                    }
                }
                return outputSet;
            }
        }
    }

    public static MaxDate maxDate(DLXPath xpath) {
        return new MaxDate(xpath);
    }

    // ////////////////////
    public static class MinDate extends XPathOp {

        DLXPath xpath;

        public MinDate(DLXPath xpath) {
            this.xpath = xpath;
            this.addChild(this.xpath);
        }

        @Override
        public String toString() {
            return "minDate(" + xpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new MinDateIterator(input, context, this.xpath);
        }

        public static class MinDateIterator extends XPathSetIter {

            public DLXPath xpath;

            public MinDateIterator(Iterator<DLNode> input, DLContext context, DLXPath xpath) {
                super(input, context);
                this.xpath = xpath;
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> outputSet = new ArrayList<DLNode>();
                Date curMaxDate = null;
                for (DLNode inputNode : inputSet) {
                    // Make sure each input is only added one time
                    boolean addedInput = false;
                    for (DLNode node : inputNode.select(context, xpath)) {
                        String value = node.getValue();
                        if (value != null) {
                            // null comes after a nonNull date.
                            Date date = null;
                            if (!Strings.isNullOrEmpty(value)) {
                                // 2014-02-24 09:31:40
                                date = com.armedica.onegate.datalayer.util.DateUtils.parse_yyyy_mm_dd_hh_mm_ss(value);
                            }
                            if (curMaxDate == null) {
                                if (date != null) {
                                    outputSet.clear();
                                }
                                curMaxDate = date;
                                if (!addedInput) {
                                    outputSet.add(inputNode);
                                    addedInput = true;
                                }
                            } else if (date != null && curMaxDate.equals(date)) {
                                curMaxDate = date;
                                if (!addedInput) {
                                    outputSet.add(inputNode);
                                    addedInput = true;
                                }
                            } else if (date != null && curMaxDate.after(date)) {
                                curMaxDate = date;
                                outputSet.clear();
                                if (!addedInput) {
                                    outputSet.add(inputNode);
                                    addedInput = true;
                                }
                            }
                        }
                    }
                }
                return outputSet;
            }
        }
    }

    public static MinDate minDate(DLXPath xpath) {
        return new MinDate(xpath);
    }

    // ////////////////////
    public static class Head extends XPathOp {

        Integer count;

        public Head(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "head(" + count + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new HeadIterator(input, context, count);
        }

        public static class HeadIterator implements Iterator<DLNode> {

            Iterator<DLNode> input;
            DLContext context;
            int count;
            int curCount = 0;

            public HeadIterator(Iterator<DLNode> input, DLContext context, int count) {
                this.input = input;
                this.context = context;
                this.count = count;
            }

            @Override
            public boolean hasNext() {
                return curCount < count && input.hasNext();
            }

            @Override
            public DLNode next() {
                if (this.hasNext()) {
                    this.curCount++;
                    return input.next();
                }
                throw new RuntimeException("no next node");
            }

            @Override
            public void remove() {
                throw new RuntimeException("not implemented");
            }
        }
    }

    public static Head head(Integer count) {
        return new Head(count);
    }

    // / -------------------------------- order by
    // burn id types called "asc", "desc",
    // burn id types called "date","string","numeric"
    // OrderBy(relativePath1, direction1, type1, relativePath2,direction2)
    public static class OrderBy extends XPathOp {

        DLXPath[] xpaths;
        List<DLXPath> selections = new ArrayList<DLXPath>();
        List<String> directions = new ArrayList<String>();
        List<String> types = new ArrayList<String>();

        public OrderBy(DLXPath... xpaths) {
            this.xpaths = xpaths;
            for (DLXPath xpath : this.xpaths) {
                this.addChild(xpath);
                DataLayerXPath.OgTree firstKid = xpath.children.get(0);
                //System.out.println("Got Class=" + firstKid.getClass().toString());
                if (firstKid instanceof DataLayerXPath.Value) {
                    DataLayerXPath.Value value = (DataLayerXPath.Value) firstKid;
                    if (StringUtility.inIgnoreCase(value.value, "asc", "desc")) {
                        directions.add(value.value.toLowerCase());
                        continue;
                    } else if (StringUtility.inIgnoreCase(value.value, "date", "string", "numeric")) {
                        types.add(value.value.toLowerCase());
                        continue;
                    }
                }
                // before adding a selection, default the previous directions
                // and types
                for (int i = directions.size(); i < selections.size(); i++) {
                    directions.add("asc");
                }
                for (int i = types.size(); i < selections.size(); i++) {
                    types.add("numeric");
                }
                selections.add(xpath);
            }
            // pad the final directions and types
            for (int i = directions.size(); i < selections.size(); i++) {
                directions.add("asc");
            }
            for (int i = types.size(); i < selections.size(); i++) {
                types.add("numeric");
            }

            // post process types,sizes,direction into a comparator
            for (int i = 0; i < this.selections.size(); i++) {

            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("orderBy(");
            for (int i = 0; i < xpaths.length; i++) {
                DLXPath xp = this.xpaths[i];
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(xp.toString());
            }
            sb.append(")");
            return sb.toString();
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new OrderByIterator(input, context, this.selections, this.directions, this.types);
        }

        public static class OrderByIterator extends XPathSetIter {

            List<DLXPath> selections;
            List<String> directions;
            List<String> types;
            MultiCompare multiCompare;

            public OrderByIterator(Iterator<DLNode> input, DLContext context, List<DLXPath> selections, List<String> directions, List<String> types) {
                super(input, context);
                this.selections = selections;
                this.directions = directions;
                this.types = types;
                List<Comparator<DLNode>> comparatorList = new ArrayList<Comparator<DLNode>>();
                for (int i = 0; i < this.selections.size(); i++) {
                    String direction = this.directions.get(i);
                    String type = this.types.get(i);
                    NodeCompare nodeCompare = new NodeCompare(direction, type);
                    NodeListCompare nodeListCompare = new NodeListCompare(nodeCompare, true);
                    DLXPath selectXPath = this.selections.get(i);
                    SelectCompare selectCompare = new SelectCompare(selectXPath, context, nodeListCompare);
                    comparatorList.add(selectCompare);
                }
                multiCompare = new MultiCompare(comparatorList);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                logger.debug("InputSetSize=" + inputSet.size());
                List<DLNode> outputSet = new ArrayList<DLNode>(inputSet);
                Collections.sort(outputSet, this.multiCompare);
                return outputSet;
            }
        }
    }

    // class knows how to compare multiple things like order by x, y, z
    public static class MultiCompare implements Comparator<DLNode> {

        List<Comparator<DLNode>> comparatorList;

        public MultiCompare(List<Comparator<DLNode>> comparatorList) {
            this.comparatorList = comparatorList;
        }

        @Override
        public int compare(DLNode o1, DLNode o2) {
            for (Comparator<DLNode> comparator : this.comparatorList) {
                int cmp = comparator.compare(o1, o2);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    }

    // give a node run select compare results
    public static class SelectCompare implements Comparator<DLNode> {

        public DLXPath xpath;
        DLContext context;
        public Comparator<List<DLNode>> nodeListComparator;

        public SelectCompare(DLXPath xpath, DLContext context, Comparator<List<DLNode>> nodeListComparator) {
            this.xpath = xpath;
            this.context = context;
            this.nodeListComparator = nodeListComparator;
        }

        @Override
        public int compare(DLNode o1, DLNode o2) {
            List<DLNode> list1 = o1.select(context, xpath);
            List<DLNode> list2 = o2.select(context, xpath);
            int cmp = this.nodeListComparator.compare(list1, list2);
            return cmp;
        }
    }

    // compare node list by comparing either the min or max node from each list
    public static class NodeListCompare implements Comparator<List<DLNode>> {

        Comparator<DLNode> nodeCompare;
        boolean compareMins;

        public NodeListCompare(Comparator<DLNode> nodeCompare, boolean compareMins) {
            this.nodeCompare = nodeCompare;
            this.compareMins = compareMins;
        }

        @Override
        public int compare(List<DLNode> o1, List<DLNode> o2) {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 != null && o2 == null) {
                return 1;
            }
            if (o1 == null && o2 != null) {
                return -1;
            }
            int s1 = o1.size();
            int s2 = o2.size();
            if (s1 == 0 && s2 == 0) {
                return 0;
            }
            if (s1 > 0 && s2 == 0) {
                return 1;
            }
            if (s1 == 0 && s2 > 0) {
                return -1;
            }
            if (this.compareMins) {
                DLNode min1 = Collections.min(o1, this.nodeCompare);
                DLNode min2 = Collections.min(o2, this.nodeCompare);
                return this.nodeCompare.compare(min1, min2);
            } else {
                DLNode max1 = Collections.max(o1, this.nodeCompare);
                DLNode max2 = Collections.max(o2, this.nodeCompare);
                return this.nodeCompare.compare(max1, max2);
            }
        }
    }

    // compare two nodes base on type and direction
    public static class NodeCompare implements Comparator<DLNode> {

        String direction;
        String type;

        public NodeCompare(String direction, String type) {
            this.direction = direction;
            this.type = type;
        }

        @Override
        public int compare(DLNode o1, DLNode o2) {
            int cmp = 0;
            boolean desc = direction.equals("desc");
            String value1 = o1.getValue();
            String value2 = o2.getValue();
            if (type.equals("date") || type.equals("string")) {
                cmp = value1.compareTo(value2);
            } else {
                BigDecimal b1 = StringUtility.toBigDecimal(value1, null);
                BigDecimal b2 = StringUtility.toBigDecimal(value2, null);
                if (b1 == null && b2 == null) {
                    cmp = 0;
                } else if (b1 != null && b2 == null) {
                    cmp = 1;
                } else if (b1 == null && b2 != null) {
                    cmp = -1;
                } else {
                    cmp = b1.compareTo(b2);
                }
            }
            if (desc) {
                cmp = 0 - cmp;
            }
            return cmp;
        }
    }

    public static OrderBy orderBy(DLXPath... xpaths) {
        return new OrderBy(xpaths);
    }

    public static OrderBy orderBy(DLXPath xpath, String direction, String type) {
        DLXPath[] xpaths = new DLXPath[]{xpath, xpath(value(direction)), xpath(value(type))};
        return new OrderBy(xpaths);
    }

    public static OrderBy orderBy(DLXPath xpath1, String direction1, String type1, DLXPath xpath2, String direction2, String type2) {
        DLXPath[] xpaths = new DLXPath[]{xpath1, xpath(value(direction1)), xpath(value(type1)), xpath2, xpath(value(direction2)), xpath(value(type2))};
        return new OrderBy(xpaths);
    }

    public static OrderBy orderBy(DLXPath xpath, String direction) {
        DLXPath[] xpaths = new DLXPath[]{xpath, xpath(value(direction))};
        return new OrderBy(xpaths);
    }

    public static OrderBy orderBy(DLXPath xpath1, String direction1, DLXPath xpath2, String direction2) {
        DLXPath[] xpaths = new DLXPath[]{xpath1, xpath(value(direction1)), xpath2, xpath(value(direction2)),};
        return new OrderBy(xpaths);
    }

    public static class ConcatenateOp extends XPathOp {

        List<DLXPath> xpaths = new ArrayList();

        /**
         * Accepts DLXPath and String as arguments
         *
         * @param arguments
         */
        public ConcatenateOp(Object... arguments) {
            if (arguments.length < 2) {
                throw new IllegalArgumentException("Must have at least two arguments to concatenate.");
            }
            boolean foundXPath = false;
            for (Object arg : arguments) {
                if (arg instanceof String) {
                    xpaths.add(new DLXPath(new Value((String) arg)));
                } else if (arg instanceof DLXPath) {
                    xpaths.add((DLXPath) arg);
                    foundXPath = true;
                } else {
                    throw new IllegalArgumentException("Concatenate only accepts arguments of type DLXPath or String");
                }
            }
            if (!foundXPath) {
                throw new IllegalArgumentException("Must pass in at least one argument of type DLXPath.");
            }
            for (DLXPath xpath : xpaths) {
                this.addChild(xpath);
            }
        }

        @Override
        public String toString() {
            String output = "";
            for (DLXPath xpath : xpaths) {
                output += xpath + ",";
            }
            output = output.substring(0, output.length() - 1);
            return "Concatenate(" + output + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ConcatenateIterator(input, context);
        }

        public class ConcatenateIterator extends XPathImmediateChildIter {

            public ConcatenateIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList();

                String out = "";
                for (DLXPath xpath : xpaths) {
                    String str = contextNode.selectValue(context, xpath);
                    if (str != null) {
                        out += str;
                    }
                }
                DLNode node = new DLValue(out);
                list.add(node);
                return list;
            }
        }
    }

    public static ConcatenateOp concatenate(Object... arguments) {
        return new ConcatenateOp(arguments);
    }

    // //-------------------------------- order by
    public static class DateAddOp extends XPathOp {

        DLXPath dateXPath;
        DLXPath typeXPath;
        DLXPath unitXPath;

        public DateAddOp(DLXPath dateXPath, DLXPath typeXPath, DLXPath unitXPath) {
            this.dateXPath = dateXPath;
            this.typeXPath = typeXPath;
            this.unitXPath = unitXPath;
            this.addChild(dateXPath);
            this.addChild(typeXPath);
            this.addChild(unitXPath);
        }

        @Override
        public String toString() {
            return "dateAdd(" + dateXPath + "," + typeXPath + "," + unitXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new DateAddIterator(input, context);
        }

        public class DateAddIterator extends XPathImmediateChildIter {

            public DateAddIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();

                // for the given context node do the date add.
                String dateStr = contextNode.selectValue(context, dateXPath);
                String type = contextNode.selectValue(context, typeXPath);
                String unitsStr = contextNode.selectValue(context, unitXPath);

                Date date = getDateOrNull(dateStr);
                if (date == null) {
                    logger.warn("Called dateAdd with invalid date [" + dateStr + "]");
                    return list;
                }
                if (type == null) {
                    logger.warn("Called dateAdd with null type");
                    return list;
                }
                if (unitsStr == null) {
                    logger.warn("Called dateAdd with null units");
                    return list;
                }
                int units = Integer.parseInt(unitsStr);

                if (type.contains("-")) {
                    type = type.substring(1);//assuming that negative operator will be put first
                    units = -units;
                }

                if (type.equalsIgnoreCase("DD")) {
                    Calendar c = Calendar.getInstance();
                    c.setTime(date);
                    c.add(Calendar.DATE, units);
                    Date outputDate = c.getTime();
                    String outputDateString = dateToString(outputDate);
                    list.add(new DLValue(outputDateString));
                } else if (type.equalsIgnoreCase("SS")) {
                    Calendar c = Calendar.getInstance();
                    c.setTime(date);
                    c.add(Calendar.SECOND, units);
                    Date outputDate = c.getTime();
                    String outputDateString = dateToString(outputDate);
                    list.add(new DLValue(outputDateString));
                } else if (type.equalsIgnoreCase("YYYY")) {
                    Calendar c = Calendar.getInstance();
                    c.setTime(date);
                    c.add(Calendar.YEAR, units);
                    Date outputDate = c.getTime();
                    String outputDateString = dateToString(outputDate);
                    list.add(new DLValue(outputDateString));
                } else {
                    throw new RuntimeException("error in dateadd, type=[" + type + "] not supported");
                }
                return list;
            }
        }
    }

    public static DateAddOp dateAdd(DLXPath dateXPath, DLXPath typeXPath, DLXPath unitXPath) {
        return new DateAddOp(dateXPath, typeXPath, unitXPath);
    }

    public static DateAddOp dateAdd(DLXPath dateXPath, String type, Integer units) {
        return new DateAddOp(dateXPath, new DLXPath(new Value(type)), new DLXPath(new Value(String.valueOf(units))));
    }

    public static DateAddOp dateAdd(DLXPath dateXPath, String type, DLXPath unitXPath) {
        return new DateAddOp(dateXPath, new DLXPath(new Value(type)), unitXPath);
    }

    // //-------------------------------- order by
    public static class DateTruncateOp extends XPathOp {

        DLXPath dateXPath;

        public DateTruncateOp(DLXPath dateXPath) {
            this.dateXPath = dateXPath;
            this.addChild(dateXPath);

        }

        @Override
        public String toString() {
            return "dateTruncate(" + dateXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new DateTruncateIterator(input, context);
        }

        public class DateTruncateIterator extends XPathImmediateChildIter {

            public DateTruncateIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();

                // for the given context node do the date add.
                String dateStr = contextNode.selectValue(context, dateXPath);
                Date date = getDateOrNull(dateStr);

                if (date != null) {
                    dateStr = dateToString(date).substring(0, 10);
                } else {
                    throw new RuntimeException("Input date is invalid and returns null.");
                }
                list.add(new DLValue(dateStr));

                return list;
            }
        }
    }

    public static DateTruncateOp dateTruncate(DLXPath dateXPath) {
        return new DateTruncateOp(dateXPath);
    }

    public static class AddAttributeOp extends XPathOp {

        DLXPath elementXpath;
        String attributeName;
        String attributeValue;

        public AddAttributeOp(DLXPath elementXpath, String attributeName, String attributeValue) {
            this.elementXpath = elementXpath;
            this.attributeName = attributeName;
            this.attributeValue = attributeValue;
            this.addChild(elementXpath);
        }

        @Override
        public String toString() {
            return "addAttribute(" + elementXpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new AddAttributeOperator(input, context);
        }

        public class AddAttributeOperator extends XPathImmediateChildIter {

            public AddAttributeOperator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {

                List<DLNode> listOfNodes = contextNode.select(context, elementXpath);
                // For each element set the attribute
                for (DLNode node : listOfNodes) {
                    if (node instanceof DLEntity) {
                        ((DLEntity) node).addAttribute(attributeName, attributeValue);
                    } else {
                        throw new RuntimeException("Add Attribute xpath does not evaluate to a DLEntity list. Xpath:" + elementXpath.toString());
                    }
                }

                return listOfNodes;
            }
        }
    }

    public static AddAttributeOp addAttribute(DLXPath entityXpath, String attributeName, String attributeValue) {
        return new AddAttributeOp(entityXpath, attributeName, attributeValue);
    }

    // //-------------------------------- order by
    public static class NumberAddOp extends XPathOp {

        DLXPath firstNumberXPath;
        DLXPath secondNumberXPath;
        DLXPath typeXPath;

        public NumberAddOp(DLXPath firstNumberXPath, DLXPath secondNumberXPath, DLXPath typeXPath) {
            this.firstNumberXPath = firstNumberXPath;
            this.secondNumberXPath = secondNumberXPath;
            this.typeXPath = typeXPath;
            this.addChild(firstNumberXPath);
            this.addChild(secondNumberXPath);
            this.addChild(typeXPath);
        }

        @Override
        public String toString() {
            return "numberAdd(" + firstNumberXPath + "," + secondNumberXPath + "," + typeXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new NumberAddIterator(input, context);
        }

        public class NumberAddIterator extends XPathImmediateChildIter {

            public NumberAddIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();

                // for the given context node do the date add.
                String firstNumStr = contextNode.selectValue(context, firstNumberXPath);
                String secNumStr = contextNode.selectValue(context, secondNumberXPath);
                String type = contextNode.selectValue(context, typeXPath);

                BigDecimal firstNum = getBigDecimalOrNull(firstNumStr);
                BigDecimal secNum = getBigDecimalOrNull(secNumStr);
                if (firstNum == null) {
                    logger.warn("Called numberAdd with invalid firsNum [" + firstNumStr + "]");
                    return list;
                }
                if (secNum == null) {
                    logger.warn("Called numberAdd with null secNumStr");
                    return list;
                }

                if (type == null) {
                    logger.warn("Called numberAdd with null type");
                    return list;
                }

                if (type.equalsIgnoreCase("add")) {
                    BigDecimal outputNumber = firstNum.add(secNum);
                    String outputNumberString = outputNumber.toPlainString();
                    list.add(new DLValue(outputNumberString));
                } else if (type.equalsIgnoreCase("subtract")) {
                    if (firstNum.compareTo(secNum) == 1) {
                        BigDecimal outputNumber = firstNum.subtract(secNum);
                        String outputNumberString = outputNumber.toPlainString();
                        list.add(new DLValue(outputNumberString));
                    } else if (firstNum.compareTo(secNum) == -1) {
                        BigDecimal outputNumber = secNum.subtract(firstNum);
                        String outputNumberString = outputNumber.toPlainString();
                        list.add(new DLValue(outputNumberString));
                    } else {
                        logger.warn("Generating a zero value for this calculation");
                        BigDecimal outputNumber = firstNum.subtract(secNum);
                        String outputNumberString = outputNumber.toPlainString();
                        list.add(new DLValue(outputNumberString));
                    }
                } else {
                    throw new RuntimeException("error in numberAdd, type=[" + type + "] not supported");
                }
                return list;
            }
        }
    }

    public static NumberAddOp numberAdd(DLXPath firstNumberXPath, DLXPath secondNumberXPath, DLXPath typeXPath) {
        return new NumberAddOp(firstNumberXPath, secondNumberXPath, typeXPath);
    }

    public static NumberAddOp numberAdd(DLXPath firstNumberXPath, DLXPath secondNumberXPath, String type) {
        return new NumberAddOp(firstNumberXPath, secondNumberXPath, new DLXPath(new Value(type)));
    }

    public static NumberAddOp numberAdd(DLXPath firstNumberXPath, String secNumStr, String type) {
        return new NumberAddOp(firstNumberXPath, new DLXPath(new Value(secNumStr)), new DLXPath(new Value(type)));
    }

    public static NumberAddOp numberAdd(DLXPath firstNumberXPath, Integer secNumStr, String type) {
        return new NumberAddOp(firstNumberXPath, new DLXPath(new Value(String.valueOf(secNumStr))), new DLXPath(new Value(type)));
    }

    // ////////////////////////////////////////////
    public static class BcOp extends XPathOp {

        DLXPath bcXPath;
        DLXPath idXPath;

        public BcOp(DLXPath bcXPath, DLXPath idXPath) {
            this.bcXPath = bcXPath;
            this.idXPath = idXPath;
        }

        @Override
        public String toString() {
            return "bc(" + bcXPath + "," + idXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new BcIterator(input, context);
        }

        public class BcIterator extends XPathImmediateChildIter {

            public BcIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();
                String bcStr = contextNode.selectValue(context, bcXPath);
                String idStr = contextNode.selectValue(context, idXPath);
                if (StringUtility.isEmpty(bcStr)) {
                    logger.warn("Called bc with null business component [" + bcStr + "] for Id [" + idStr + "]");
                    return list;
                }
                if (StringUtility.isEmpty(idStr)) {
                    logger.warn("Called bc with null business component Id [" + idStr + "] for business component [" + bcStr + "]");
                    return list;
                }
                logger.debug("Lookup BC name=[" + bcStr + "] id=[" + idStr + "]");
                DLEntity entity = context.dataLayerSlice.dataLayer.staticEntityCache.getSiebelEntity(bcStr, idStr);
                logger.debug("Lookup BC name=[" + bcStr + "] id=[" + idStr + "] returning entity.isNull=" + (entity == null ? "Y" : "N"));
                list.add(entity);
                return list;
            }
        }
    }

    // BusinessComponentName,RowId
    public static BcOp bc(DLXPath bcSiebelNameXPath, DLXPath idXPath) {
        return new BcOp(bcSiebelNameXPath, idXPath);
    }

    public static BcOp bc(String bcSiebelName, DLXPath idXPath) {
        DLXPath bcSiebelNameXPath = xpath(value(bcSiebelName));
        return new BcOp(bcSiebelNameXPath, idXPath);
    }

    // /////////////////////////
    public static class BcLookupOp extends XPathOp {

        DLXPath bcXPath;
        DLXPath[] lookupFields;

        public BcLookupOp(DLXPath bcXPath, DLXPath... lookupFields) {
            this.bcXPath = bcXPath;
            this.lookupFields = lookupFields;
            if (this.lookupFields.length % 2 != 0) {
                throw new RuntimeException("Error with bcLookup [" + this.toString() + "]. We expect and even number of FieldName, FieldValue pairs");
            }
        }

        @Override
        public String toString() {
            return "bcLookup(" + bcXPath + "," + Joiner.on(",").join(lookupFields) + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new BcLookupIterator(input, context);
        }

        public class BcLookupIterator extends XPathImmediateChildIter {

            public BcLookupIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();
                String busCompName = contextNode.selectValue(context, bcXPath);
                if (StringUtility.isEmpty(busCompName)) {
                    logger.warn("Called lookupBC with null business component [" + busCompName + "]");
                    return list;
                }
                Map<String, String> lookupFieldMap = new HashMap<String, String>();
                for (int i = 0; i < lookupFields.length; i++) {
                    String fieldName = contextNode.selectValue(context, lookupFields[i++]);
                    if (StringUtility.isEmpty(fieldName)) {
                        logger.warn("Called lookupBC with null fieldName [" + fieldName + "] for business component [" + busCompName + "]");
                        return list;
                    }
                    String fieldValue = contextNode.selectValue(context, lookupFields[i]);
                    lookupFieldMap.put(fieldName, fieldValue);
                }
                logger.debug("Lookup BC name=[" + busCompName + "] fields=[" + lookupFieldMap + "]");
                List<DLEntity> entities = context.dataLayerSlice.dataLayer.staticEntityLookupCache.getSiebelEntity(busCompName, lookupFieldMap);
                list.addAll(entities);
                logger.debug("Lookup BC name=[" + busCompName + "] fields=[" + lookupFieldMap + "] returning count=" + list.size());
                return list;
            }
        }
    }

    public static BcLookupOp bcLookup(String bcSiebelName) {
        return bcLookup(bcSiebelName, new DLXPath[]{});
    }

    public static BcLookupOp bcLookup(DLXPath bcSiebelNameXPath, DLXPath... lookupFields) {
        return new BcLookupOp(bcSiebelNameXPath, lookupFields);
    }

    public static BcLookupOp bcLookup(String bcSiebelName, DLXPath... lookupFields) {
        DLXPath bcSiebelNameXPath = xpath(value(bcSiebelName));
        return new BcLookupOp(bcSiebelNameXPath, lookupFields);
    }

    public static BcLookupOp bcLookup(String bcSiebelName, String bcFieldName, DLXPath bcFieldValueXPath) {
        DLXPath bcSiebelNameXPath = xpath(value(bcSiebelName));
        DLXPath bcFieldNameXPath = xpath(value(bcFieldName));
        return new BcLookupOp(bcSiebelNameXPath, new DLXPath[]{bcFieldNameXPath, bcFieldValueXPath});
    }

    public static BcLookupOp bcLookup(String bcSiebelName, String bcFieldName, String bcFieldValue) {
        DLXPath bcSiebelNameXPath = xpath(value(bcSiebelName));
        DLXPath bcFieldNameXPath = xpath(value(bcFieldName));
        DLXPath bcFieldValueXPath = xpath(value(bcFieldValue));
        return new BcLookupOp(bcSiebelNameXPath, new DLXPath[]{bcFieldNameXPath, bcFieldValueXPath});
    }

    // /////////////////////////
    public static class SpawnChildEntityOp extends XPathOp {

        DLXPath linkNameXPath;
        DLXPath nameXPath;
        DLXPath implXPath;
        DLXPath baseEntityXPath;

        public SpawnChildEntityOp(DLXPath linkNameXPath, DLXPath nameXPath, DLXPath implXPath, DLXPath baseEntityXPath) {
            this.linkNameXPath = linkNameXPath;
            this.nameXPath = nameXPath;
            this.implXPath = implXPath;
            this.baseEntityXPath = baseEntityXPath;
        }

        @Override
        public String toString() {
            return "spawn_entity(" + linkNameXPath + "," + nameXPath + "," + implXPath + "," + baseEntityXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new SpawnChildEntityIterator(input, context);
        }

        public class SpawnChildEntityIterator extends XPathImmediateChildIter {

            public SpawnChildEntityIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();
                String parentEntityName = ((DLEntity) contextNode).getTagName();
                String parentLinkName = contextNode.selectValue(context, linkNameXPath);
                String entityName = contextNode.selectValue(context, nameXPath);
                String entityImpl = contextNode.selectValue(context, implXPath);

                VirtualEntityDefinition virtualEntityDef = context.getDataLayerSlice().getDataLayer().configuration.getVirtualEntityDefinition(parentEntityName, parentLinkName, entityName, entityImpl);
                for (DLNode node : contextNode.select(context, baseEntityXPath)) {
                    if (!(node instanceof DLEntity)) {
                        throw new RuntimeException("Error spawning entity [" + entityName + "] [" + entityImpl + "] cannot select base entities using xpath [" + baseEntityXPath + "] because it returned a non entity node:" + node.toString());
                    }
                    DLEntity baseEntity = (DLEntity) node;
                    if (baseEntity.getTagName().equals(virtualEntityDef.baseEntity)) {
                        VirtualEntity virtualEntity = virtualEntityDef.getEntity(baseEntity);
                        logger.debug("[spawn_child_entity]Created entity [" + virtualEntity.debugId() + "] from implementation [" + virtualEntityDef.implementation + "]");
                        list.add(virtualEntity);
                        virtualEntity.setParentNode(contextNode);
                    } else {
                        throw new RuntimeException("Bad entity configuration, selected entity [" + node.getTagName() + "] does not match expected base entity tag [" + virtualEntityDef.baseEntity + "] for link [" + parentLinkName + "]");
                    }
                }
                return list;
            }
        }
    }

    public static SpawnChildEntityOp spawn_child_entity(String parentLinkName, String entityName, String implementation, DLXPath baseEntityXPath) {
        DLXPath parentLinkNameXPath = xpath(value(parentLinkName));
        DLXPath entityNameXPath = xpath(value(entityName));
        DLXPath implementationXPath = xpath(value(implementation));
        return new SpawnChildEntityOp(parentLinkNameXPath, entityNameXPath, implementationXPath, baseEntityXPath);
    }
    // /////////////////////////

    /**
     * Pipeline operator that runs the debug on the inputs and returns them
     * untouched. You can insert this in any pipeline and it will not change
     * behavior, it just gives you printing.
     *
     * @author Josh Monzon
     */
    public static class DebugOp extends XPathOp {

        Object[] args;

        public DebugOp(Object... args) {
            this.args = args;
        }

        @Override
        public String toString() {
            return "debug(" + StringUtility.joinStrings(",", CollectionUtility.toArrayList(args)) + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new DebugIterator(input, context);
        }

        public class DebugIterator extends PassThruIter {

            public DebugIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public void run(DLNode ogEntityNode, DLContext context) {
                if (context != null && context.ruleContext != null && context.ruleContext.debugEnabled()) {
                    // System.out.print(ogEntityNode.toXmlString());
                    List<DLNode> ogEntityNodeList = new ArrayList<DLNode>();
                    ogEntityNodeList.add(ogEntityNode);
                    String debugStr = context.ruleContext.getCurrentRuleRule().getSetString(context, ogEntityNodeList, args);
                    context.ruleContext.getCurrentRuleRule().debug(debugStr);
                } else if (DLContext.logger.isDebugEnabled()) {
                    String debugStr = ogEntityNode.selectConcat(context, args);
                    DLContext.logger.debug(debugStr);
                }
            }
        }
    }

    public static DebugOp debug(Object... args) {
        return new DebugOp(args);
    }

    // ///////////////////
    public static class ToXmlOp extends XPathOp {

        DLXPath xpath = null;

        public ToXmlOp(DLXPath xpath) {
            this.xpath = xpath;
        }

        @Override
        public String toString() {
            return "toXml(" + xpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ToXmlOpIterator(input, context);
        }

        public class ToXmlOpIterator extends XPathSetIter {

            public ToXmlOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> out = new ArrayList<DLNode>();
                List<String> list = new ArrayList<String>();
                for (DLNode inputNode : inputSet) {
                    if (ToXmlOp.this.xpath == null) {
                        list.add(inputNode.toXmlString());
                    } else {
                        for (DLNode selectedNode : inputNode.select(context, ToXmlOp.this.xpath)) {
                            list.add(selectedNode.toXmlString());
                        }
                    }
                }
                String string = StringUtility.joinStrings(StringUtility.NL, list);
                DLNode value = new DLValue(string);
                out.add(value);
                return out;
            }

        }
    }

    public static ToXmlOp toXml() {
        return toXml(null);
    }

    public static ToXmlOp toXml(DLXPath xpath) {
        return new ToXmlOp(xpath);
    }

    // //////////////////////////////
    public static class GetTagNameOp extends XPathOp {

        DLXPath xpath = null;

        public GetTagNameOp(DLXPath xpath) {
            this.xpath = xpath;
        }

        @Override
        public String toString() {
            return "getTagName(" + xpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new GetTagNameOpIterator(input, context);
        }

        public class GetTagNameOpIterator extends XPathSetIter {

            public GetTagNameOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> out = new ArrayList<DLNode>();
                for (DLNode inputNode : inputSet) {
                    if (GetTagNameOp.this.xpath == null) {
                        out.add(new DLValue((inputNode.getTagName())));
                    } else {
                        for (DLNode selectedNode : inputNode.select(context, GetTagNameOp.this.xpath)) {
                            out.add(new DLValue((selectedNode.getTagName())));
                        }
                    }
                }
                return out;
            }

        }
    }

    public static GetTagNameOp getTagName() {
        return getTagName(null);
    }

    public static GetTagNameOp getTagName(DLXPath xpath) {
        return new GetTagNameOp(xpath);
    }

    public static class SubstringOp extends XPathOp {

        DLXPath xpath = null;
        int start = -1;
        int end = -1;

        public SubstringOp(DLXPath xpath, int start, int end) {
            this.xpath = xpath;
            this.start = start;
            this.end = end;
            if (start > end) {
                 throw new IllegalArgumentException("Please make sure that the beginning index is less than or equal to the ending index.");
            } else if (start ==-1 || end ==-1){
                throw new IllegalArgumentException("Please make sure to input start and end indices.");
            }
        }

        @Override
        public String toString() {
            return "Substring(" + xpath + " Substring start " + start + " Substring end " + end + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new SubstringOpIterator(input, context);
        }

        public class SubstringOpIterator extends XPathSetIter {

            public SubstringOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> out = new ArrayList<DLNode>();
                for (DLNode inputNode : inputSet) {
                    for (DLNode selectedNode : inputNode.select(context, SubstringOp.this.xpath)) {
                        String value = selectedNode.getValue();
                        if (value == null) {
                            value = "";
                        }
                        int length = value.length();
                        int startClone = start;
                        int endClone = end;
                        if (end == -1) {
                            endClone = length;
                        }
                        if (length < start) {
                            startClone = length;
                            endClone = length;
                        } else {
                            if (length < end) {
                                endClone = length;
                            }
                        }
                        out.add(new DLValue(value.substring(startClone, endClone)));
                    }
                }
                return out;
            }
        }
    }

    public static SubstringOp subString(DLXPath xpath, Integer start, Integer end) {
        return new SubstringOp(xpath, start, end);
    }



    // //////////////////////////////
    // //////////////////////////////
    public static class FieldOp extends XPathOp {

        DLXPath nameXPath;
        DLXPath valueXPath;

        public FieldOp(DLXPath nameXPath, DLXPath valueXPath) {
            this.nameXPath = nameXPath;
            this.valueXPath = valueXPath;
        }

        @Override
        public String toString() {
            return "field(" + nameXPath + "," + this.valueXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new FieldOpIterator(input, context);
        }

        public class FieldOpIterator extends XPathSetIter {

            public FieldOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> out = new ArrayList<DLNode>();
                for (DLNode inputNode : inputSet) {
                    if (FieldOp.this.nameXPath != null) {
                        String name = inputNode.selectValue(context, FieldOp.this.nameXPath);
                        if (StringUtility.notEmpty(name)) {
                            if (FieldOp.this.valueXPath != null) {
                                String value = inputNode.selectValue(context, FieldOp.this.valueXPath);
                                if (value == null) {
                                    value = "";
                                }
                                DLField field = new DLFieldBase(null, name, value);
                                field.setDefaultContext(this.context);
                                out.add(field);
                            }
                        }
                    }
                }
                return out;
            }
        }
    }

    public static FieldOp field(String nameXPath, DLXPath valueXPath) {
        return field(xpath(value(nameXPath)), valueXPath);
    }

    public static FieldOp field(DLXPath nameXPath, DLXPath valueXPath) {
        return new FieldOp(nameXPath, valueXPath);
    }

    public static class LinkOp extends XPathOp {

        DLXPath nameXPath;
        DLXPath valueXPath;

        public LinkOp(DLXPath nameXPath, DLXPath valueXPath) {
            this.nameXPath = nameXPath;
            this.valueXPath = valueXPath;
        }

        @Override
        public String toString() {
            return "link(" + nameXPath + "," + this.valueXPath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new LinkOpIterator(input, context);
        }

        public class LinkOpIterator extends XPathSetIter {

            public LinkOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> out = new ArrayList<DLNode>();
                for (DLNode inputNode : inputSet) {
                    if (LinkOp.this.nameXPath != null) {
                        String name = inputNode.selectValue(context, LinkOp.this.nameXPath);
                        if (StringUtility.notEmpty(name)) {
                            DLLink link = new DLLinkBase(null, name);
                            link.setDefaultContext(this.context);
                            out.add(link);
                            if (LinkOp.this.valueXPath != null) {
                                for (DLNode linkedNode : inputNode.select(context, LinkOp.this.valueXPath)) {
                                    link.appendChild(linkedNode);
                                }
                            }
                        }
                    }
                }
                return out;
            }
        }
    }

    public static LinkOp link(String nameXPath, DLXPath valueXPath) {
        return link(xpath(value(nameXPath)), valueXPath);
    }

    public static LinkOp link(DLXPath nameXPath, DLXPath valueXPath) {
        return new LinkOp(nameXPath, valueXPath);
    }

    // //////////////////////////////
    public static SiebelEntityOp siebel_entity(Object... args) {
        String entityName = (String) args[0];
        XPathOp[] fieldOps = new XPathOp[args.length - 1];
        for (int i = 1; i < args.length; i++) {
            if (args[i] instanceof XPathOp) {
                fieldOps[i - 1] = (XPathOp) args[i];
            } else {
                throw new RuntimeException("Invalid call to entity() xpath operator. Arguments are expected to be XPathOps such as field() or link()");
            }
        }
        return siebel_entity((String) entityName, fieldOps);
    }

    public static SiebelEntityOp siebel_entity(String entityName, XPathOp... fieldOps) {
        return siebel_entity(xpath(value(entityName)), fieldOps);
    }

    public static SiebelEntityOp siebel_entity(DLXPath nameXPath, XPathOp... fieldOps) {
        return new SiebelEntityOp(nameXPath, fieldOps);
    }

    public static class SiebelEntityOp extends EntityOp {

        public SiebelEntityOp(DLXPath nameXPath, XPathOp... childOps) {
            super(nameXPath, childOps);
        }

        @Override
        public String toString() {
            return "siebel_entity(" + nameXPath + "," + Joiner.on(",").join(this.childOps) + ")";
        }

        @Override
        DLEntity createEntity(DLContext context, String name) {
            if (context != null) {
                DLEntity entity = context.dataLayerSlice.dataLayer.siebelBusCompConstructor.construct(context, name);
                return entity;
            } else {
                throw new RuntimeException("Cannot create siebel bus comp entity when context is null");
            }

        }

    }

    // //////////////////////////////
    public static class EntityOp extends XPathOp {

        DLXPath nameXPath;
        XPathOp[] childOps;

        public EntityOp(DLXPath nameXPath, XPathOp... childOps) {
            this.nameXPath = nameXPath;
            this.childOps = childOps;
        }

        @Override
        public String toString() {
            return "entity(" + nameXPath + "," + Joiner.on(",").join(this.childOps) + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new EntityOpIterator(input, context);
        }

        public class EntityOpIterator extends XPathSetIter {

            public EntityOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> out = new ArrayList<DLNode>();
                for (DLNode inputNode : inputSet) {
                    if (EntityOp.this.nameXPath != null) {
                        String name = inputNode.selectValue(context, EntityOp.this.nameXPath);
                        if (StringUtility.notEmpty(name)) {
                            DLEntity entity = createEntity(context, name);

                            for (XPathOp childOp : EntityOp.this.childOps) {
                                DLNode fieldNode = childOp.iterator(context, Iterators.forArray(inputNode)).next();
                                if (fieldNode != null) {
                                    entity.appendChild(context, fieldNode);
                                    fieldNode.setParentNode(entity);
                                }
                            }
                            out.add(entity);
                        }
                    }
                }
                return out;
            }
        }

        DLEntity createEntity(DLContext context, String name) {
            DLEntity entity;
            if (context != null && context.dataLayerSlice != null) {
                entity = context.dataLayerSlice.createEntity(name);
            } else {
                entity = new DLEntityBase(context, name);
            }
            return entity;
        }
    }

    public static EntityOp entity(Object... args) {
        String entityName = (String) args[0];
        XPathOp[] fieldOps = new XPathOp[args.length - 1];
        for (int i = 1; i < args.length; i++) {
            if (args[i] instanceof XPathOp) {
                fieldOps[i - 1] = (XPathOp) args[i];
            } else {
                throw new RuntimeException("Invalid call to entity() xpath operator. Arguments are expected to be XPathOps such as field() or link()");
            }
        }
        return entity((String) entityName, fieldOps);
    }

    public static EntityOp entity(String entityName, XPathOp... fieldOps) {
        return entity(xpath(value(entityName)), fieldOps);
    }

    public static EntityOp entity(DLXPath nameXPath, XPathOp... fieldOps) {
        return new EntityOp(nameXPath, fieldOps);
    }

    // //////////////////////////////
    public static class GroupByOp extends XPathOp {

        List<DLXPath> xpaths;
        List<String> aliases;

        public GroupByOp(List<DLXPath> xpaths, List<String> aliases) {
            this.xpaths = xpaths;
            this.aliases = aliases;
        }

        @Override
        public String toString() {
            return "groupBy(" + xpaths + "," + aliases + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new GroupByOpIterator(input, context);
        }

        public class GroupByOpIterator extends XPathSetIter {

            public GroupByOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                // group the inputs
                Map<HashableArrayList<String>, List<DLNode>> groups = new HashMap<HashableArrayList<String>, List<DLNode>>();
                for (DLNode inputNode : inputSet) {
                    List<String> compoundKeyFields = new ArrayList<String>();
                    for (DLXPath xpath : GroupByOp.this.xpaths) {
                        String value = inputNode.selectValue(context, xpath);
                        if (value == null) {
                            value = "";
                        }
                        compoundKeyFields.add(value);
                    }
                    HashableArrayList<String> compoundKey = new HashableArrayList<String>(compoundKeyFields);
                    List<DLNode> groupList = groups.get(compoundKey);
                    if (groupList == null) {
                        groupList = new ArrayList<DLNode>();
                        groups.put(compoundKey, groupList);
                    }
                    groupList.add(inputNode);
                }
                // wrap each group in a <group> node
                List<DLNode> out = new ArrayList<DLNode>();
                for (HashableArrayList<String> compoundKey : groups.keySet()) {
                    List<DLNode> groupList = groups.get(compoundKey);
                    DLElementBase groupElement = new DLElementBase("group");
                    for (int i = 0; i < compoundKey.list.size(); i++) {
                        String alias = GroupByOp.this.aliases.get(i);
                        String value = compoundKey.list.get(i);
                        groupElement.addChildNode(alias, value);
                    }
                    for (DLNode item : groupList) {
                        DLNode origParent = item.getParentNode();
                        groupElement.addChildNode(item);
                        item.setParentNode(origParent);
                    }
                    out.add(groupElement);
                }
                return out;
            }

        }
    }

    public static GroupByOp groupBy(DLXPath xpath, String alias) {
        List<DLXPath> xpaths = new ArrayList<DLXPath>();
        List<String> aliases = new ArrayList<String>();
        xpaths.add(xpath);
        aliases.add(alias);
        return new GroupByOp(xpaths, aliases);
    }

    // ///////////////// select new
    // one in zero or 1 out.
    public static class SelectNew extends XPathOp {

        DLFactory[] factories;

        public SelectNew(DLFactory[] factories) {
            this.factories = factories;
        }

        @Override
        public String toString() {
            return "SelectNew(" + factories + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new SelectNewIterator(input, context, factories);
        }

        public static class SelectNewIterator implements Iterator<DLNode> {

            Iterator<DLNode> input;
            DLNode contextNode;
            DLContext context;
            DLFactory[] factories;
            int factoryIndex = -1;
            Queue<DLNode> output = new LinkedList<DLNode>();

            public SelectNewIterator(Iterator<DLNode> input, DLContext context, DLFactory[] factories) {
                this.input = input;
                this.context = context;
                this.factories = factories;
            }

            @Override
            public boolean hasNext() {
                for (;;) {
                    if (!this.output.isEmpty()) {
                        return true;
                    }
                    if (contextNode != null && ++this.factoryIndex < this.factories.length) {
                        DLFactory factory = this.factories[this.factoryIndex];
                        for (DLNode node : factory.createNodes(context, contextNode)) {
                            output.add(node);
                        }
                        if (!output.isEmpty()) {
                            return true;
                        }
                        continue;
                    }
                    if (input.hasNext()) {
                        contextNode = input.next();
                        this.factoryIndex = -1;
                        continue;
                    }
                    return false;
                }
            }

            @Override
            public DLNode next() {
                if (this.hasNext()) {
                    return output.poll();
                }
                throw new RuntimeException("no next node");
            }

            @Override
            public void remove() {
                throw new RuntimeException("not implemented");
            }
        }
    }

    public static SelectNew selectNew(DLFactory... factories) {
        return new SelectNew(factories);
    }

    // /////////////////////////
    public static class ParameterOp extends XPathOp {

        final String category;
        final String parameter;
        final DLXPath defaultValueXPath;
        final DLXPath selectXPath;

        public ParameterOp(String category, String parameter, DLXPath defaultValueXPath) {
            this.category = category;
            this.parameter = parameter;
            this.defaultValueXPath = defaultValueXPath;
            this.selectXPath = xpath(root("DataLayerSlice"), child("Parameters"), child(category), child(parameter));
        }

        @Override
        public String toString() {
            if (this.defaultValueXPath != null) {
                return "parameterLookup(\"" + category + "\",\"" + parameter + "\"," + this.defaultValueXPath + ")";
            } else {
                return "parameterLookup(\"" + category + "\",\"" + parameter + "\")";
            }
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ParameterOpIterator(input, context);
        }

        public class ParameterOpIterator extends XPathImmediateChildIter {

            public ParameterOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = contextNode.select(context, ParameterOp.this.selectXPath);
                if (list.size() == 0 && ParameterOp.this.defaultValueXPath != null) {
                    list = contextNode.select(ParameterOp.this.defaultValueXPath);
                }
                return list;
            }
        }
    }

    public static ParameterOp parameter(String category, String parameter, DLXPath defaultValueXPath) {
        return new ParameterOp(category, parameter, defaultValueXPath);
    }

    public static ParameterOp parameter(String category, String parameter, String defaultValue) {
        DLXPath defaultValueXPath = null;
        if (defaultValue != null) {
            defaultValueXPath = xpath(value(defaultValue));
        }
        return new ParameterOp(category, parameter, defaultValueXPath);
    }

    public static ParameterOp parameter(String category, String parameter) {
        return new ParameterOp(category, parameter, null);
    }

    // /////////////////////////
    // /////////////////////////
    public static class MessageOp extends XPathOp {

        final String category;
        final String message;
        final DLXPath defaultValueXPath;
        final DLXPath selectXPath;

        public MessageOp(String category, String message, DLXPath defaultValueXPath) {
            this.category = category;
            this.message = message;
            this.defaultValueXPath = defaultValueXPath;
            this.selectXPath = xpath(root("DataLayerSlice"), child("Messages"), child(category), child(message));
        }

        @Override
        public String toString() {
            if (this.defaultValueXPath != null) {
                return "messageLookup(\"" + category + "\",\"" + message + "\"," + this.defaultValueXPath + ")";
            } else {
                return "messageLookup(\"" + category + "\",\"" + message + "\")";
            }
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new MessageOpIterator(input, context);
        }

        public class MessageOpIterator extends XPathImmediateChildIter {

            public MessageOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = contextNode.select(context, MessageOp.this.selectXPath);
                if (list.size() == 0 && MessageOp.this.defaultValueXPath != null) {
                    list = contextNode.select(MessageOp.this.defaultValueXPath);
                }
                return list;
            }
        }
    }

    public static MessageOp message(String category, String message, DLXPath defaultValueXPath) {
        return new MessageOp(category, message, defaultValueXPath);
    }

    public static MessageOp message(String category, String message, String defaultValue) {
        DLXPath defaultValueXPath = null;
        if (defaultValue != null) {
            defaultValueXPath = xpath(value(defaultValue));
        }
        return new MessageOp(category, message, defaultValueXPath);
    }

    public static MessageOp message(String category, String message) {
        return new MessageOp(category, message, null);
    }

    // /////////////////////////
    public static class LovOp extends XPathOp {

        final String category;
        final String lov;
        final DLXPath defaultValueXPath;
        final DLXPath selectXPath;

        public LovOp(String category, String lov, DLXPath defaultValueXPath) {
            this.category = category;
            this.lov = lov;
            this.defaultValueXPath = defaultValueXPath;
            this.selectXPath = xpath(root("DataLayerSlice"), child("ListOfValues"), child(category), child(lov));
        }

        @Override
        public String toString() {
            if (this.defaultValueXPath != null) {
                return "lov(\"" + category + "\",\"" + lov + "\"," + this.defaultValueXPath + ")";
            } else {
                return "lov(\"" + category + "\",\"" + lov + "\")";
            }
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new LovOpIterator(input, context);
        }

        public class LovOpIterator extends XPathImmediateChildIter {

            public LovOpIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = contextNode.select(context, LovOp.this.selectXPath);
                if (list.size() == 0 && LovOp.this.defaultValueXPath != null) {
                    list = contextNode.select(LovOp.this.defaultValueXPath);
                }
                return list;
            }
        }
    }

    public static LovOp lov(String category, String lov, DLXPath defaultValueXPath) {
        return new LovOp(category, lov, defaultValueXPath);
    }

    public static LovOp lov(String category, String lov, String defaultValue) {
        DLXPath defaultValueXPath = null;
        if (defaultValue != null) {
            defaultValueXPath = xpath(value(defaultValue));
        }
        return new LovOp(category, lov, defaultValueXPath);
    }

    public static LovOp lov(String category, String lov) {
        return new LovOp(category, lov, null);
    }

    // /////////////////////////
    public static class ChildOrNewElement extends XPathOp {

        final String operator;
        final String tagName;

        public ChildOrNewElement(String operator, String nameMatch) {
            this.operator = operator;
            this.tagName = nameMatch;
        }

        public ChildOrNewElement(String nameMatch) {
            this("childOrNewElement", nameMatch);
        }

        @Override
        public String toString() {
            return operator + "(" + tagName + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ChildOrNewLinkIterator(input, context, this.tagName);
        }

        DLNode createNewElement(DLContext context, DLNode parentNode) {
            return new DLEntityBase(this.tagName);
        }

        public class ChildOrNewLinkIterator extends XPathSetIter {

            public String tagName;

            public ChildOrNewLinkIterator(Iterator<DLNode> input, DLContext context, String nameMatch) {
                super(input, context);
                this.tagName = nameMatch;
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> outputSet = new ArrayList<>();
                for (DLNode contextNode : inputSet) {
                    List<DLNode> list = contextNode.getChildNodes(context, tagName);
                    if (list == null) {
                        logger.debug("");
                    }
                    if (list.isEmpty()) {
                        DLNode newElement = createNewElement(context, contextNode);
                        DLNode added = contextNode.appendChild(context, newElement);
                        outputSet.add(added);
                    } else {
                        outputSet.addAll(list);
                    }
                }
                return outputSet;
            }

        }
    }

    public static ChildOrNewElement childOrNewElement(String tagName) {
        return new ChildOrNewElement(tagName);
    }

    ////
    public static class ChildOrNewLink extends ChildOrNewElement {

        public ChildOrNewLink(String tagName) {
            super("childOrNewLink", tagName);
        }

        @Override
        DLNode createNewElement(DLContext context, DLNode parentNode) {
            if (!(parentNode instanceof DLEntity)) {
                throw new IllegalArgumentException();
            }
            DLEntity parentEntity = (DLEntity) parentNode;
            DLLinkBase link = new DLLinkBase(parentEntity, this.tagName);
            link.setDefaultContext(context);
            return link;
        }

    }

    public static ChildOrNewLink childOrNewLink(String tagName) {
        return new ChildOrNewLink(tagName);
    }

    ////
    public static class ChildOrNewEntity extends ChildOrNewElement {

        public ChildOrNewEntity(String tagName) {
            super("childOrNewEntity", tagName);
        }

        @Override
        DLNode createNewElement(DLContext context, DLNode parentNode) {
            DLEntity entity;
            if (context != null && context.dataLayerSlice != null) {
                entity = context.dataLayerSlice.createEntity(this.tagName);
            } else {
                entity = new DLEntityBase(context, this.tagName);
            }
            return entity;
        }

    }

    public static ChildOrNewEntity childOrNewEntity(String nameMatch) {
        return new ChildOrNewEntity(nameMatch);
    }

    ////
    public static class ChildOrNewField extends ChildOrNewElement {

        public ChildOrNewField(String tagName) {
            super("childOrNewField", tagName);
        }

        @Override
        DLNode createNewElement(DLContext context, DLNode parentNode) {
            DLFieldBase field;
            field = new DLFieldBase(parentNode, this.tagName, "");
            return field;
        }

    }

    public static ChildOrNewField childOrNewField(String nameMatch) {
        return new ChildOrNewField(nameMatch);
    }

    /////////
    public static class ChildNewElement extends XPathOp {

        final String operator;
        final String tagName;

        public ChildNewElement(String operator, String nameMatch) {
            this.operator = operator;
            this.tagName = nameMatch;
        }

        public ChildNewElement(String nameMatch) {
            this("childNewElement", nameMatch);
        }

        @Override
        public String toString() {
            return operator + "(" + tagName + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new ChildNewLinkIterator(input, context, this.tagName);
        }

        DLNode createNewElement(DLContext context, DLNode parentNode) {
            return new DLEntityBase(this.tagName);
        }

        public class ChildNewLinkIterator extends XPathSetIter {

            public String tagName;

            public ChildNewLinkIterator(Iterator<DLNode> input, DLContext context, String nameMatch) {
                super(input, context);
                this.tagName = nameMatch;
            }

            @Override
            List<DLNode> getOutputSet(List<DLNode> inputSet) {
                List<DLNode> outputSet = new ArrayList<>();
                for (DLNode contextNode : inputSet) {
                    DLNode newElement = createNewElement(context, contextNode);
                    DLNode added = contextNode.appendChild(context, newElement);
                    outputSet.add(added);
                }
                return outputSet;
            }

        }
    }

    public static ChildNewElement childNewElement(String tagName) {
        return new ChildNewElement(tagName);
    }

    ////
    public static class ChildNewLink extends ChildNewElement {

        public ChildNewLink(String tagName) {
            super("childNewLink", tagName);
        }

        @Override
        DLNode createNewElement(DLContext context, DLNode parentNode) {
            if (!(parentNode instanceof DLEntity)) {
                throw new IllegalArgumentException();
            }
            DLEntity parentEntity = (DLEntity) parentNode;
            DLLinkBase link = new DLLinkBase(parentEntity, this.tagName);
            link.setDefaultContext(context);
            return link;
        }

    }

    public static ChildNewLink childNewLink(String tagName) {
        return new ChildNewLink(tagName);
    }

    ////
    public static class ChildNewEntity extends ChildNewElement {

        public ChildNewEntity(String tagName) {
            super("childNewEntity", tagName);
        }

        @Override
        DLNode createNewElement(DLContext context, DLNode parentNode) {
            DLEntity entity;
            if (context != null && context.dataLayerSlice != null) {
                entity = context.dataLayerSlice.createEntity(this.tagName);
            } else {
                entity = new DLEntityBase(context, this.tagName);
            }
            return entity;
        }

    }

    public static ChildNewEntity childNewEntity(String nameMatch) {
        return new ChildNewEntity(nameMatch);
    }

    /////////
    public static class SetVarOp extends XPathOp {

        String varName;

        public SetVarOp(String varName) {
            this.varName = varName;
        }

        @Override
        public String toString() {
            return "setVar(" + this.varName + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new setVarIterator(input, context);
        }

        public class setVarIterator extends PassThruIter {

            public setVarIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public void run(DLNode currentNode, DLContext context) {
                if (context != null) {
                    context.setVariable(varName, currentNode);
                } else {
                    throw new RuntimeException("Cannot setVar when context is null");
                }
            }
        }
    }

    public static SetVarOp setVar(String varName) {
        return new SetVarOp(varName);
    }

    public static class BooleanDebugOp extends BooleanOp {

        String text;
        BooleanOp op;

        public BooleanDebugOp(String text, BooleanOp op) {
            super();
            this.op = op;
            this.text = text;
            this.addChildren(op);
        }

        @Override
        public boolean evaluate(DLNode contextNode, DLContext context) {
            boolean result = op.evaluate(contextNode, context);
            String debugStr = this.text + result;
            if (context != null && context.ruleContext != null && context.ruleContext.debugEnabled()) {
                context.ruleContext.getCurrentRuleRule().debug(debugStr);
            } else if (DLContext.logger.isDebugEnabled()) {
                DLContext.logger.debug(debugStr);
            }
            return result;
        }

        @Override
        public String toString() {
            return "debug_bool(" + text + "," + op + ")";
        }
    }

    public static BooleanDebugOp debug_bool(String text, BooleanOp op) {
        return new BooleanDebugOp(text, op);
    }

    public static class GetPersonAgeOp extends XPathOp {

        DLXPath dateOfBirth;
        DLXPath relativeDate;

        public GetPersonAgeOp(DLXPath dateXPath, DLXPath typeXPath) {
            this.dateOfBirth = dateXPath;
            this.relativeDate = typeXPath;
            this.addChild(dateXPath);
            this.addChild(typeXPath);
        }

        @Override
        public String toString() {
            return "GetPersonAge(" + dateOfBirth + "," + relativeDate + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new GetPersonAgeIterator(input, context);
        }

        public class GetPersonAgeIterator extends XPathImmediateChildIter {

            public GetPersonAgeIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();

                // for the given context node do the date add.
                String dateOfBirthStr = contextNode.selectValue(context, dateOfBirth);
                String relativeDateStr = contextNode.selectValue(context, relativeDate);

                if (dateOfBirthStr == null || dateOfBirthStr.equals("")) {
                    logger.warn("Called GetPersonAge with invalid date [" + dateOfBirthStr + "]");
                    return list;
                }
                if (relativeDateStr == null || relativeDateStr.equals("")) {
                    logger.warn("Called GetPersonAge with no relative date");
                    return list;
                }

                LocalDate dob = SiebelTypesUtility.convertStringToLocalDate(dateOfBirthStr);
                LocalDate relativeDate = SiebelTypesUtility.convertStringToLocalDate(relativeDateStr);
                String outputAgeString = Integer.toString(DateUtils.GetAge(dob, relativeDate));
                list.add(new DLValue(outputAgeString));
                return list;
            }
        }
    }

    public static GetPersonAgeOp getPersonAge(DLXPath dateXPath, DLXPath typeXPath) {
        return new GetPersonAgeOp(dateXPath, typeXPath);
    }

    public static class GetPersonAgeHIOp extends XPathOp {

        DLXPath dateOfBirth;
        DLXPath relativeDate;

        public GetPersonAgeHIOp(DLXPath dateXPath, DLXPath typeXPath) {
            this.dateOfBirth = dateXPath;
            this.relativeDate = typeXPath;
            this.addChild(dateXPath);
            this.addChild(typeXPath);
        }

        @Override
        public String toString() {
            return "GetPersonAgeHI(" + dateOfBirth + "," + relativeDate + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new GetPersonAgeHIIterator(input, context);
        }

        public class GetPersonAgeHIIterator extends XPathImmediateChildIter {

            public GetPersonAgeHIIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList<DLNode>();

                // for the given context node do the date add.
                String dateOfBirthStr = contextNode.selectValue(context, dateOfBirth);
                String relativeDateStr = contextNode.selectValue(context, relativeDate);

                if (dateOfBirthStr == null || dateOfBirthStr.equals("")) {
                    logger.warn("Called GetPersonAgeHI with invalid date [" + dateOfBirthStr + "]");
                    return list;
                }
                if (relativeDateStr == null || relativeDateStr.equals("")) {
                    logger.warn("Called GetPersonAgeHI with no relative date");
                    return list;
                }

                LocalDate dob = SiebelTypesUtility.convertStringToLocalDate(dateOfBirthStr);
                LocalDate relativeDate = SiebelTypesUtility.convertStringToLocalDate(relativeDateStr);
                String outputAgeString = Integer.toString(DateUtils.GetAge(dob, relativeDate, "Hawaii"));
                list.add(new DLValue(outputAgeString));
                return list;
            }
        }
    }

    public static GetPersonAgeHIOp getPersonAgeHI(DLXPath dateXPath, DLXPath typeXPath) {
        return new GetPersonAgeHIOp(dateXPath, typeXPath);
    }

    public static CountOp count(DLXPath xpath) {
        return new CountOp(xpath);
    }

    public static class CountOp extends XPathOp {

        DLXPath xpath;

        public CountOp(DLXPath xpath) {
            this.xpath = xpath;
            this.addChild(this.xpath);
        }

        @Override
        public String toString() {
            return "count(" + xpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new CountIterator(input, context);
        }

        public class CountIterator extends XPathImmediateChildIter {

            public CountIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList(1);
                int count = contextNode.select(xpath).size();
                DLNode node = new DLValue(String.valueOf(count));
                list.add(node);
                return list;
            }
        }

    }

    public static CountDistinctOp countDistinct(DLXPath xpath) {
        return new CountDistinctOp(xpath);
    }

    public static class CountDistinctOp extends XPathOp {

        DLXPath xpath;

        public CountDistinctOp(DLXPath xpath) {
            this.xpath = xpath;
            this.addChild(this.xpath);
        }

        @Override
        public String toString() {
            return "countDistinct(" + xpath + ")";
        }

        @Override
        public String toStringTree() {
            return this.toString();
        }

        @Override
        public Iterator<DLNode> iterator(DLContext context, Iterator<DLNode> input) {
            return new CountDistinctIterator(input, context);
        }

        public class CountDistinctIterator extends XPathImmediateChildIter {

            public CountDistinctIterator(Iterator<DLNode> input, DLContext context) {
                super(input, context);
            }

            @Override
            public List<DLNode> selectShallow(DLNode contextNode) {
                List<DLNode> list = new ArrayList(1);
                Set<String> distinctNodes = new HashSet();
                for (DLNode node : contextNode.select(xpath)) {
                    String value = node.getValue();
                    distinctNodes.add(value);
                }
                int count = distinctNodes.size();
                DLNode node = new DLValue(String.valueOf(count));
                list.add(node);
                return list;
            }
        }

    }

}
