package com.armedica.onegate.datalayer.main;

import static com.armedica.onegate.datalayer.util.StringUtility.notEmpty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.Name;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPBodyElement;
import javax.xml.soap.SOAPConstants;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPHeader;
import javax.xml.soap.SOAPHeaderElement;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.w3c.dom.*;
import org.apache.log4j.Logger;

import com.armedica.onegate.datalayer.caching.CacheAdaptor;
import com.armedica.onegate.datalayer.caching.ForeverCache;
import com.armedica.onegate.datalayer.caching.HashableArray;
import com.armedica.onegate.datalayer.caching.ICacheUser;
import com.armedica.onegate.datalayer.config.ConfigRow;
import com.armedica.onegate.datalayer.config.ConfigSheet;
import com.armedica.onegate.datalayer.config.DataLayerConfig;
import com.armedica.onegate.datalayer.logging.OgLogger;
import com.armedica.onegate.datalayer.logging.OgLoggerFactory;
import com.armedica.onegate.datalayer.main.SiebelBusCompMetaData.BusCompMultiValueLinkFieldsCache.BusCompMultiValueLinkFields;
import com.armedica.onegate.datalayer.main.SiebelBusCompMetaData.BusCompMultiValueLinksCache.BusCompMultiValueLink;
import com.armedica.onegate.datalayer.main.SiebelWebServices.DirtyPath.Operation;
import com.armedica.onegate.datalayer.main.SiebelWebServices.WebService.Port.WorkFlowProcess;
import com.armedica.onegate.datalayer.main.SiebelWebServices.WebService.Port.WorkFlowProcess.WorkFlowProcessProps;
import com.armedica.onegate.datalayer.main.SiebelWebServices.WebService.RepositoryIntegrationObject.RepositoryIntegrationComponent;
import com.armedica.onegate.datalayer.main.SiebelWebServices.WebService.RepositoryIntegrationObject.RepositoryIntegrationComponent.RepositoryIntegrationComponentField;
import com.armedica.onegate.datalayer.main.SiebelWebServices.WebService.RepositoryIntegrationObject.RepositoryIntegrationComponent.RepositoryIntegrationComponentKey;
import com.armedica.onegate.datalayer.main.SiebelWebServices.WebService.RepositoryIntegrationObject.RepositoryIntegrationComponent.RepositoryIntegrationComponentKey.RepositoryIntegrationComponentKeyField;
import com.armedica.onegate.datalayer.main.SiebelWebServices.WebService.RepositoryIntegrationObject.RepositoryIntegrationComponent.RepositoryIntegrationComponentUserProp;
import com.armedica.onegate.datalayer.util.CollectionUtility;
import com.armedica.onegate.datalayer.util.SiebelTypesUtility;
import com.armedica.onegate.datalayer.util.StringUtility;
import com.armedica.onegate.datalayer.util.XmlUtility;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This integrates the data layer with the persistence layer.
 * Factors in all deltas within the data layer and converts this into web servcie calls to Siebel. 
 * 
 * @author Josh Monzon
 * 
 */
public class SiebelWebServices {

    final static Logger logger = Logger.getLogger(SiebelWebServices.class);
    final DataLayer dataLayer;
    final WebServiceCache WebServiceCache;
    private final List<PersistDependency> persistDependencies;

    public SiebelWebServices(DataLayer dataLayer) {
        this.dataLayer = dataLayer;
        this.WebServiceCache = new WebServiceCache(this.dataLayer);

        this.persistDependencies = loadPersistDependencies();
    }

    private List<PersistDependency> loadPersistDependencies() {
        List<PersistDependency> dependencies = new ArrayList();

        // Pull Persist Dependencies from ConfigSheet "Persist Dependency"
        DataLayerConfig oneGateConfig = dataLayer.getConfiguration().getOneGateConfig();
        if (oneGateConfig != null) {
            ConfigSheet configSheet = dataLayer.getConfiguration().getOneGateConfig().getSheet("Persist Dependency");
            if (configSheet != null) {
                for (ConfigRow row : configSheet.getRows()) {
                    if (row.notIgnored()) {
                        String persistEntity = row.getField(("Persist Entity"));
                        Operation persistOperation = DirtyPath.Operation.valueOf(row.getField("Persist Operation"));
                        String dependsOnEntity = row.getField("Depends On Entity");
                        Operation dependsOnOperation = DirtyPath.Operation.valueOf(row.getField("Depends On Operation"));
                        dependencies.add(new PersistDependency(persistEntity, persistOperation, dependsOnEntity, dependsOnOperation));
                    }
                }
            }
        }

        return dependencies;
    }

    public static class PersistDependency {

        String persistedEntity;
        DirtyPath.Operation persistedOperation;
        String dependentdEntity;
        DirtyPath.Operation dependentOperation;

        public PersistDependency(String persistedEntity, DirtyPath.Operation persistedOperation, String dependentdEntity, DirtyPath.Operation dependentOperation) {
            this.persistedEntity = persistedEntity;
            this.persistedOperation = persistedOperation;
            this.dependentdEntity = dependentdEntity;
            this.dependentOperation = dependentOperation;
        }
    }

    // define a cache or web services
    public static class WebServiceCache implements ICacheUser<WebServiceCache.Key, WebService> {

        final static OgLogger logger = OgLoggerFactory.getLogger(WebServiceCache.class);
        final DataLayer dataLayer;
        public CacheAdaptor<Key, WebService> cache = null;
        final String cacheName = "WebServiceCache";

        public WebServiceCache(DataLayer dataLayer) {
            this.dataLayer = dataLayer;
            ForeverCache<Key, WebService> cacheAlgorithm = new ForeverCache<Key, WebService>(cacheName);
            cacheAlgorithm.setNumTries(1);
            cache = new CacheAdaptor<>(this.dataLayer.cacheMaster, this, cacheAlgorithm);
        }

        @Override
        public WebService missGet(Key key) {
            String serviceName = key.array[0];
            synchronized (this) {
                WebService existing = this.cache.getCachedValue(key);
                if (existing != null) {
                    logger.debug("Someon already build webservice:" + serviceName);
                    return existing;
                }

                WebService webService = new WebService(this.dataLayer.staticSlice, serviceName);
                return webService;
            }
        }

        public WebService getWebService(String serviceName) {
            return cache.get(new Key(serviceName));
        }

        public static class Key extends HashableArray<String> {

            public Key(String name) {
                super(name);
            }
        }
    }

    // TBD- load these from configuration
    final List<String> webServiceNames = Lists.newArrayList(
            "OneGate Application Data Submission Service",
            "OneGate Contact Data Submission Service",
            //"OneGate FINS Group Policy Data Submission Service",
            "OneGate HLS Case Data Submission Service",
            "OneGate Master Case Data Submission Service"
    //"OneGate User Account Data Service"
    );

    public boolean hasDependentDirtyPath(DirtyPath dirtyPath, List<DirtyPath> inputDirtyPaths) {
        String persistedEntity = dirtyPath.getDirtySiebelBusComp().tagName;
        Operation persistedOperation = dirtyPath.operation;

        for (PersistDependency persistDependency : persistDependencies) {
            if (persistDependency.persistedEntity.equals(persistedEntity)
                    && persistDependency.persistedOperation.equals(persistedOperation)) {
                for (DirtyPath dPath : inputDirtyPaths) {
                    if (dPath.getDirtySiebelBusComp().tagName.equals(persistDependency.dependentdEntity) && dPath.operation.equals(persistDependency.dependentOperation)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public List<DirtyPath> getHandledDirthPaths(WebService webService, List<DirtyPath> inputDirtyPaths) {
        List<DirtyPath> clonedInputDirtyPaths = new ArrayList<>(inputDirtyPaths);
        List<DirtyPath> outputDirtyPaths = new ArrayList<>();
        // Apply all the rules HERE!
        // 1: each web service knows the paths it can process
        // 2: assoc requires a parent!
        // 3: you can only insert an entity instance 1x in a given call
        // 4: you can only setMVG if there are no pending update or inserts for that same BC or any of it's children
        // 5: you cannot associate if you have a pending insert with a different parent
        // 6: you can only associate if there is no pending associate for the same parent
        // 7: you can only setMVG if there are no pending setMVGs for the same parent
        // 8: you can only update an entity instance if it hasn't been updated already in a different dirty path
        // 9. you can only delete an entity if there are no pending updates or inserts for that entity and it's children
        // 10. you can only delete an entity if that entity has a parent
        // 11. you can only delete an entity that doesn't have pending associate, delete, or setMVG dirtyPaths to the same parent, you can delete it in a later call
        // 12. you can only delete an entity if that entity is not a parent entity (or grand parent) of another entity in another delete dirtyPath because this 
        //     will break the chain!
        // 13. you can only process a dirty path, if the entity operation has no pending dependent entity operation in our list of pending dirty paths as defined in
        //     the entityPersistDependencies HashMap  
        // This is a set of business component entities that we are inserting for a given list of dirty path and their respective parents
        Map<DLEntity, DLEntity> insertedEntities = new HashMap<>();
        // This is a set of business component entities that we are associating for a given list of dirty path and their respective parents
        Map<DLEntity, List<DLEntity>> associatedEntities = new HashMap<>();
        // This is a set of business component entities that we are setMVGing for a given list of dirty path and their respective parents
        Map<DLEntity, List<DLEntity>> setMVGEntities = new HashMap<>();
        // This is a set of business component entities that we are deleting for a given list of dirty path and their respective parents
        Map<DLEntity, List<DLEntity>> deletedEntities = new HashMap<>();
        // This is a set of business component entities that we are updating for a given list of dirty path
        Set<DLEntity> updatedEntities = new HashSet<>();
        // This is a set of the names of the business components of all dirty paths with nonMVGOperations tied to them
        Set<String> updateOrInsertOperationBCs = new HashSet<>();
        // This is a set of the names of the business components of all dirty paths with nonMVGOperations tied to them
        Set<DLEntity> cantDeleteEntities = new HashSet<>();

        // First process all insert and update dirty paths
        for (DirtyPath dirtyPath : clonedInputDirtyPaths) {

            if (dirtyPath.isInsert() || dirtyPath.isUpdate()) {
                // Rule 13: If this specific entity and its operation has an unprocessed dependent entity and operation, then we can't process this dirty path
                if (hasDependentDirtyPath(dirtyPath, inputDirtyPaths)) {
                    continue;
                }

                // First and 2nd rule: Check if this webservice can process a given dirtyPath in our list. 
                // If it can be processed, keep applying more rules.
                if (!webService.canProcessPath(dirtyPath)) {
                    continue;
                }

                // If a dirtyBC is update or insert, make sure that all of it's parent/grandparent BCs are all marked as being
                // used for an insert or update operation. This means that a setMVG operation for these parent/grandparent BCs 
                // can't be processed by this web service.
                int rootIdx = dirtyPath.bcNames.indexOf(webService.repositoryIntegrationObject.getRootBusCompName());
                for (int dirtyIdx = rootIdx; dirtyIdx <= dirtyPath.getMaxIndex(); dirtyIdx++) {
                    String busCompName = dirtyPath.bcNames.get(dirtyIdx);
                    updateOrInsertOperationBCs.add(busCompName);
                }

                // Third rule: Make sure that you only insert an entity instance 1 time for a given call
                if (dirtyPath.isInsert()) {
                    // Here we check if the current dirty path's specific business component entity has 
                    // been marked for insertion already. If it's marked already, then don't include this dirty path in our output.
                    if (insertedEntities.containsKey(dirtyPath.getDirtySiebelBusComp())) {
                        continue;
                    } // If it hasn't been marked for processing then mark it for processing
                    else {
                        insertedEntities.put(dirtyPath.getDirtySiebelBusComp(), dirtyPath.getParentOfDirtySiebelBusComp());
                        outputDirtyPaths.add(dirtyPath);
                        logger.debug("Added dirtypath [" + dirtyPath.toString() + "] to the list of dirtypaths that webservice [" + webService.toString() + "] can process. ");
                    }
                }
                // Rule 8: If the current entity has already been updated - no need to update it again!
                if (dirtyPath.isUpdate()) {
                    if (updatedEntities.contains(dirtyPath.getDirtySiebelBusComp())) {
                        continue;
                    } else {
                        updatedEntities.add(dirtyPath.getDirtySiebelBusComp());
                        outputDirtyPaths.add(dirtyPath);
                        logger.debug("Added dirtypath [" + dirtyPath.toString() + "] to the list of dirtypaths that webservice [" + webService.toString() + "] can process. ");
                    }

                }
            }
        }

        clonedInputDirtyPaths.removeAll(outputDirtyPaths);

        // Now process all non-insert or update dirty paths
        for (DirtyPath dirtyPath : clonedInputDirtyPaths) {
            // Rule 13: If this specific entity and its operation has an unprocessed dependent entity and operation, then we can't process this dirty path
            if (hasDependentDirtyPath(dirtyPath, inputDirtyPaths)) {
                continue;
            }

            if (!webService.canProcessPath(dirtyPath)) {
                continue;
            }
            // Rule 4: Make sure that setMVGs are only processed if the corresponding dirtypath BC
            // hasn't been used for any non SetMVG or non Associate operations! This is because in our downstream logic, we always pick the
            // non-MVG ICs for non MVG operations. This can cause issues when we are mixing ICs for non-MVG and MVG operations
            if (dirtyPath.isSetMVG()) {

                if (updateOrInsertOperationBCs.contains(dirtyPath.getDirtySiebelBusComp().getNativeName())) {
                    continue;
                }
                // Rule 7: Make sure that setMVGs are only processed if the current child entity hasn't been marked for SetMVG for the same parent
                // in a separate dirty path
                if (setMVGEntities.containsKey(dirtyPath.getDirtySiebelBusComp())
                        && setMVGEntities.get(dirtyPath.getDirtySiebelBusComp()) != null
                        && dirtyPath.getParentOfDirtySiebelBusComp() != null
                        && setMVGEntities.get(dirtyPath.getDirtySiebelBusComp()).contains(dirtyPath.getParentOfDirtySiebelBusComp())) {
                    continue;
                } // If it hasn't been marked for processing then mark it for processing
                else {
                    CollectionUtility.append(setMVGEntities, dirtyPath.getDirtySiebelBusComp(), dirtyPath.getParentOfDirtySiebelBusComp());
                    outputDirtyPaths.add(dirtyPath);
                    logger.debug("Added dirtypath [" + dirtyPath.toString() + "] to the list of dirtypaths that webservice [" + webService.toString() + "] can process. ");
                }
            }
            // Rule 5: We can ONLY associate an entity to it's parent entity if that entity isn't being inserted to a different
            // parent entity somewhere in our list of dirty paths. If it is, we can't process it in this web-service.
            if (dirtyPath.isAssociate()) {
                DLEntity entity = dirtyPath.getDirtySiebelBusComp();
                DLEntity parentEntity = null;
                if (dirtyPath.entities.size() > 1) {
                    parentEntity = dirtyPath.entities.get(dirtyPath.entities.size() - 2);
                }
                // Here we check if that entity has been inserted already and if it has been, check if it's being inserted to a different parent.
                // If that's the case then don't process this in this web-service.
                if (insertedEntities.containsKey(entity) && parentEntity != null && insertedEntities.get(entity) != null && !insertedEntities.get(entity).equals(parentEntity)) {
                    continue;
                }

                // Rule 6: We can ONLY associate an entity to it's parent entity if that entity isn't being associated to the same parent. 
                // parent entity somewhere in our list of dirty paths. 
                // Here we check if the entity has been associated to the same parent already.
                if (associatedEntities.containsKey(dirtyPath.getDirtySiebelBusComp())
                        && associatedEntities.get(dirtyPath.getDirtySiebelBusComp()) != null
                        && dirtyPath.getParentOfDirtySiebelBusComp() != null
                        && associatedEntities.get(dirtyPath.getDirtySiebelBusComp()).contains(dirtyPath.getParentOfDirtySiebelBusComp())) {
                    continue;
                } // If it hasn't been marked for processing then mark it for processing
                else {
                    CollectionUtility.append(associatedEntities, dirtyPath.getDirtySiebelBusComp(), dirtyPath.getParentOfDirtySiebelBusComp());
                    outputDirtyPaths.add(dirtyPath);
                    logger.debug("Added dirtypath [" + dirtyPath.toString() + "] to the list of dirtypaths that webservice [" + webService.toString() + "] can process. ");
                }
            }
        }
        clonedInputDirtyPaths.removeAll(outputDirtyPaths);

        // Now processes dirtyPaths that are being deleted
        for (DirtyPath dirtyPath : clonedInputDirtyPaths) {
            // Rule 13: If this specific entity and its operation has an unprocessed dependent entity and operation, then we can't process this dirty path
            if (hasDependentDirtyPath(dirtyPath, inputDirtyPaths)) {
                continue;
            }

            if (!webService.canProcessPath(dirtyPath)) {
                continue;
            }

            if (dirtyPath.isDelete()) {
                DLEntity entity = dirtyPath.getDirtySiebelBusComp();
                // Rule 9: You can only delete an entity if that entity if there are no pending updates or inserts for that entity
                if (updatedEntities.contains(entity) || insertedEntities.containsKey(entity)) {
                    continue;
                }
                // Rule 10: You can only delete an entity if that entity has a parent
                DLEntity parentEntity = dirtyPath.getParentOfDirtySiebelBusComp();
                if (parentEntity == null && !entity.getTagName().equals("HLSCase")) {
                    continue;
                }

                // Rule 11. you can only delete an entity that doesn't have pending associates, setMVG, or delete dirtyPaths to the same parent. you can delete it in a later call
                if ((associatedEntities.containsKey(entity)
                        && associatedEntities.get(entity) != null
                        && associatedEntities.get(entity).contains(parentEntity))
                        || (setMVGEntities.containsKey(entity)
                        && setMVGEntities.get(entity) != null
                        && setMVGEntities.get(entity).contains(parentEntity))
                        || (deletedEntities.containsKey(entity)
                        && deletedEntities.get(entity) != null
                        && deletedEntities.get(entity).contains(parentEntity))) {
                    continue;
                }
                // Rule 12. you can only delete an entity if that entity is not a parent entity (or grand parent) of another entity in another delete dirtyPath because this 
                //     will break the chain!
                if (cantDeleteEntities.contains(entity)) {
                    continue;
                } else {
                    // If a dirtyBC is a delete make sure that it parent/grandparent BCs are all marked as being already
                    // used for a delete operation. This means that we cannot delete any of those BCs in the same call.
                    int rootIdx = dirtyPath.bcNames.indexOf(webService.repositoryIntegrationObject.getRootBusCompName());
                    for (int dirtyIdx = rootIdx; dirtyIdx < dirtyPath.entities.size() - 1; dirtyIdx++) {
                        DLEntity cantDeleteEntity = dirtyPath.entities.get(dirtyIdx);
                        cantDeleteEntities.add(cantDeleteEntity);
                    }
                    CollectionUtility.append(deletedEntities, entity, parentEntity);
                    outputDirtyPaths.add(dirtyPath);
                    logger.debug("Added dirtypath [" + dirtyPath.toString() + "] to the list of dirtypaths that webservice [" + webService.toString() + "] can process. ");
                }
            }
        }

        // Before we finalize our list to be deleted, make sure that we enforce rule 12 strictly as we may have missed some dirty Paths that shouldn't be deleted!
        for (DirtyPath dirtyPath : new ArrayList<>(outputDirtyPaths)) {
            if (dirtyPath.isDelete() && cantDeleteEntities.contains(dirtyPath.getDirtySiebelBusComp())) {
                outputDirtyPaths.remove(dirtyPath);
            }
        }
        return outputDirtyPaths;
    }

    public List<Map.Entry<WebService, List<DirtyPath>>> buildOptimizedWebServiceCalls(List<DirtyPath> inputDirtyPaths) {
        Heap h = new Heap();
        List<Map.Entry<WebService, List<DirtyPath>>> orderedServices = new ArrayList<>();
        List<DirtyPath> remainingDirtyPaths = new ArrayList<>(inputDirtyPaths);
        List<DirtyPath> runningRemainingDirtyPaths = new ArrayList<>(inputDirtyPaths);
        Set<List<WebService>> visitedStrategies = new HashSet<>();

        if (remainingDirtyPaths.size() > 0) {
            WebServiceCallStrategy baseStrategy = new WebServiceCallStrategy(remainingDirtyPaths, orderedServices);
            h.add(baseStrategy);

            while (!h.isEmpty()) {
                WebServiceCallStrategy top = (WebServiceCallStrategy) h.top();
                h.pop();

                // Make sure we don't vist the same strategy twice
                if (visitedStrategies.contains(top.strategy)) {
                    continue;
                }
                visitedStrategies.add(top.strategy);

                // Check if we still have any remainingDirtyPaths to process and if so, return the strategy to get there!
                if (top.remainingdirtyPaths.isEmpty()) {
                    return top.wsCalls;
                }

                // Loop through each webService and try using each of them to process!
                for (String webServiceName : this.webServiceNames) {
                    WebService webService = this.WebServiceCache.getWebService(webServiceName);
                    List<DirtyPath> handledPaths = this.getHandledDirthPaths(webService, top.remainingdirtyPaths);

                    // Check if selected web service even works! If it doesn't even handle any path, go to the next web service.
                    if (handledPaths.isEmpty()) {
                        continue;
                    }

                    List<DirtyPath> newRemainingDirtyPaths = new ArrayList<>(top.remainingdirtyPaths);
                    removeDuplicateDirtyPaths(newRemainingDirtyPaths, handledPaths);
                    List<Map.Entry<WebService, List<DirtyPath>>> newWSCalls = new ArrayList<>(top.wsCalls);
                    Map.Entry<WebService, List<DirtyPath>> newWSCall = new SimpleEntry<>(webService, handledPaths);
                    newWSCalls.add(newWSCall);
                    h.add(new WebServiceCallStrategy(newRemainingDirtyPaths, newWSCalls));
                }

                runningRemainingDirtyPaths = top.remainingdirtyPaths;
            }

            String remainingDPaths = "";
            for (DirtyPath runningRemainingDirtyPath : runningRemainingDirtyPaths) {
                remainingDPaths = runningRemainingDirtyPath.toString() + "\n" + remainingDPaths;
            }
            logger.debug("We have some dirtyPaths that cannot be processed. A web service call strategy cannot be found to process the following dirty Paths: \n" + remainingDPaths);
            throw new RuntimeException("Cannot process all dirty Paths:" + remainingDPaths);
        }
        logger.debug("No dirtypath to process here.");
        return orderedServices;
    }

    public void removeDuplicateDirtyPaths(List<DirtyPath> inputDirtyPaths, List<DirtyPath> handledDirtyPaths) {
        // First blindly remove all handled dirty paths from our input list of dirty paths. 
        inputDirtyPaths.removeAll(handledDirtyPaths);
        List<DirtyPath> duplicateDirtyPaths = new ArrayList<>();

        // Let's loop through our handled dirty paths
        for (DirtyPath handledDirtyPath : handledDirtyPaths) {
            // Now we handle the scenario where inserted entities in our handled dirty paths might be inserted to other parent entities.
            // We want to make sure we remove those too! If our handledDirtyPath is an insert, then let's check our list of input paths 
            // and see if another insert exists for the same business component entity  but could be a different parent
            if (handledDirtyPath.isInsert()) {
                for (DirtyPath inputDirtyPath : inputDirtyPaths) {
                    if (inputDirtyPath.isInsert() && handledDirtyPath.getDirtySiebelBusComp().equals(inputDirtyPath.getDirtySiebelBusComp())) {
                        duplicateDirtyPaths.add(inputDirtyPath);
                    }
                }
            }
            // Now we handle the scenario where updated entities in our handled dirty paths might be updated in other dirty paths.
            // If an entity is already updated, make sure we don't use up a web service to update it again!
            if (handledDirtyPath.isUpdate()) {
                for (DirtyPath inputDirtyPath : inputDirtyPaths) {
                    if (inputDirtyPath.isUpdate() && handledDirtyPath.getDirtySiebelBusComp().equals(inputDirtyPath.getDirtySiebelBusComp())) {
                        duplicateDirtyPaths.add(inputDirtyPath);
                    }
                }
            }

            // Now we handle the scenario where associated entities in our handled dirty paths might be inserted to the same parent entities
            // in a different dirty path. We want to make sure we remove those too! If our handledDirtyPath is an associate, then let's check our list of input paths 
            // and see if another associate exists for the same business component entity and same parent
            if (handledDirtyPath.isAssociate()) {
                for (DirtyPath inputDirtyPath : inputDirtyPaths) {
                    if (inputDirtyPath.isAssociate()
                            && handledDirtyPath.getDirtySiebelBusComp().equals(inputDirtyPath.getDirtySiebelBusComp())
                            && handledDirtyPath.getParentOfDirtySiebelBusComp() != null
                            && inputDirtyPath.getParentOfDirtySiebelBusComp() != null
                            && handledDirtyPath.getParentOfDirtySiebelBusComp().equals(inputDirtyPath.getParentOfDirtySiebelBusComp())) {
                        duplicateDirtyPaths.add(inputDirtyPath);
                    }
                }
            }
            // Now we handle the scenario where setMVG entities in our handled dirty paths might be setMVG to the same parent entities
            // in a different dirty path. We want to make sure we remove those too! If our handledDirtyPath is a setMVG, then let's check our list of input paths 
            // and see if another setMVG exists for the same business component entity and same different parent
            if (handledDirtyPath.isSetMVG()) {
                for (DirtyPath inputDirtyPath : inputDirtyPaths) {
                    if (inputDirtyPath.isSetMVG()
                            && handledDirtyPath.getDirtySiebelBusComp().equals(inputDirtyPath.getDirtySiebelBusComp())
                            && handledDirtyPath.getParentOfDirtySiebelBusComp() != null
                            && inputDirtyPath.getParentOfDirtySiebelBusComp() != null
                            && handledDirtyPath.getParentOfDirtySiebelBusComp().equals(inputDirtyPath.getParentOfDirtySiebelBusComp())) {
                        duplicateDirtyPaths.add(inputDirtyPath);
                    }
                }
            }
            // Now we handle the scenario where delete entities in our handled dirty paths might be deleted to the same parent entities
            // in a different dirty path. We want to make sure we remove those too! If our handledDirtyPath is a delete, then let's check our list of input paths 
            // and see if another delete exists for the same business component entity and same different parent
            if (handledDirtyPath.isDelete()) {
                for (DirtyPath inputDirtyPath : inputDirtyPaths) {
                    if (inputDirtyPath.isDelete()
                            && handledDirtyPath.getDirtySiebelBusComp().equals(inputDirtyPath.getDirtySiebelBusComp())
                            && handledDirtyPath.getParentOfDirtySiebelBusComp() != null
                            && inputDirtyPath.getParentOfDirtySiebelBusComp() != null
                            && handledDirtyPath.getParentOfDirtySiebelBusComp().equals(inputDirtyPath.getParentOfDirtySiebelBusComp())) {
                        duplicateDirtyPaths.add(inputDirtyPath);
                    }
                }
            }
        }

        // Now remove all duplicate inputDirtyPaths
        inputDirtyPaths.removeAll(duplicateDirtyPaths);
    }

    public boolean persistEntity(Slice slice, List<SiebelBusCompEntity> entities) {
        List<DirtyPath> dirtyPaths = this.getDirtyPaths(entities);
        if (dirtyPaths.size() == 0) {
            logger.debug("No dirty paths found");
            return false;
        }
        logger.debug("Found [" + dirtyPaths.size() + "] dirty paths");
        for (DirtyPath dirtyPath : dirtyPaths) {
            logger.debug(dirtyPath);
        }

        List<Map.Entry<WebService, List<DirtyPath>>> orderedServices = buildOptimizedWebServiceCalls(dirtyPaths);
        logger.debug("Chosen Strategy");
        for (Map.Entry<WebService, List<DirtyPath>> m : orderedServices) {
            String dirtyPathString = "";
            for (DirtyPath d : m.getValue()) {
                dirtyPathString = d.toString() + "\n" + dirtyPathString;
            }
            logger.debug("Webservice: " + m.getKey().name + " Handled Paths:\n" + dirtyPathString);
        }
        // fire the ordered service calls
        for (Map.Entry<WebService, List<DirtyPath>> selectedService : orderedServices) {
            String webServiceName = selectedService.getKey().name;
            List<DirtyPath> webServiceDirtyPaths = selectedService.getValue();
            this.persistDirtyPaths(slice, webServiceName, webServiceDirtyPaths);
        }
        return true;
    }

    public boolean oldPersistEntity(Slice slice, List<SiebelBusCompEntity> entities) {
        List<DirtyPath> dirtyPaths = this.getDirtyPaths(entities);
        if (dirtyPaths.size() == 0) {
            logger.debug("No dirty paths found");
            return false;
        }
        logger.debug("Founds [" + dirtyPaths.size() + "] dirty paths");
        for (DirtyPath dirtyPath : dirtyPaths) {
            logger.debug(dirtyPath);
        }

        // 1: each web service knows the paths it can process
        //    assoc requires a parent!
        // 2: you can only insert and entity instance 1x in a given call
        // 3: call the services with the most paths first
        // 4: break ties based on min wasted levels
        Map<WebService, List<DirtyPath>> serviceDirtyPaths = Maps.newHashMap();

        List<DirtyPath> cannotProcess = new ArrayList<>();
        for (DirtyPath dirtyPath : dirtyPaths) {
            boolean found = false;
            for (String webServiceName : this.webServiceNames) {
                WebService webService = this.WebServiceCache.getWebService(webServiceName);
                if (webService.canProcessPath(dirtyPath)) {
                    CollectionUtility.append(serviceDirtyPaths, webService, dirtyPath);
                    found = true;
                }
            }
            if (!found) {
                cannotProcess.add(dirtyPath);
            }
        }
        if (cannotProcess.size() > 0) {
            logger.error("Error cannot find service that can persist these dirty paths:");
            for (DirtyPath dirtyPath : cannotProcess) {
                logger.error(dirtyPath.toString());
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Each web service knows the paths it can process");
            for (Map.Entry<WebService, List<DirtyPath>> e1 : serviceDirtyPaths.entrySet()) {
                logger.debug("Web Service:" + e1.getKey().name);
                for (DirtyPath dp : e1.getValue()) {
                    logger.debug("DirtyPath:" + dp);
                }
            }
        }

        for (Map.Entry<WebService, List<DirtyPath>> e1 : serviceDirtyPaths.entrySet()) {
            List<DirtyPath> duplicateEntityInsertDirtyPaths = new ArrayList<>();
            {
                Set<DLEntity> busComps = new HashSet<>();
                for (DirtyPath dirtyPath : e1.getValue()) {
                    if (dirtyPath.isInsert()) {
                        if (busComps.contains(dirtyPath.getDirtySiebelBusComp())) {
                            duplicateEntityInsertDirtyPaths.add(dirtyPath);
                        } else {
                            busComps.add(dirtyPath.getDirtySiebelBusComp());
                        }
                    }
                }
            }
            e1.getValue().removeAll(duplicateEntityInsertDirtyPaths);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("you can only insert an entity instance 1x in a given call");
            for (Map.Entry<WebService, List<DirtyPath>> e1 : serviceDirtyPaths.entrySet()) {
                logger.debug("Web Service:" + e1.getKey().name);
                for (DirtyPath dp : e1.getValue()) {
                    logger.debug("DirtyPath:" + dp);
                }
            }
        }

        // build the correct sequence of service calls to do the persistence
        List<Map.Entry<WebService, List<DirtyPath>>> orderedServices = new ArrayList<>();

        // order the services based on most paths, then by fewest wasted.
        while (serviceDirtyPaths.size() > 0) {

            int maxPaths = 0;
            for (List<DirtyPath> list : serviceDirtyPaths.values()) {
                maxPaths = list.size() > maxPaths ? list.size() : maxPaths;
            }
            //int maxPaths = serviceDirtyPaths.values().stream().mapToInt(v -> v.size()).max().getAsInt();
//            List<Map.Entry<WebService, List<DirtyPath>>> maxServices = serviceDirtyPaths.entrySet().stream()
//                    .filter(e -> e.getValue().size() == maxPaths).collect(Collectors.toList());

            List<Map.Entry<WebService, List<DirtyPath>>> maxServices = new ArrayList<>();
            for (Map.Entry<WebService, List<DirtyPath>> e : serviceDirtyPaths.entrySet()) {
                if (e.getValue().size() == maxPaths) {
                    maxServices.add(e);
                }
            }

            Map.Entry<WebService, List<DirtyPath>> selectedService = null;
            if (maxServices.size() == 1) {
                selectedService = maxServices.get(0);
            } else {
                //selectedService = maxServices.stream().min((a, b) -> closestToServiceRoot(a) - closestToServiceRoot(b)).get();
                int min = Integer.MAX_VALUE;
                for (Map.Entry<WebService, List<DirtyPath>> ss : maxServices) {
                    int depth = closestToServiceRoot(ss);
                    if (depth < min) {
                        min = depth;
                        selectedService = ss;
                    }
                }

            }
            // save the service call
            orderedServices.add(selectedService);
            // remove this service because we are done with it
            serviceDirtyPaths.remove(selectedService.getKey());
            // remove this services dirty paths from the remaining services
            for (WebService key : Lists.newArrayList(serviceDirtyPaths.keySet())) {
                List<DirtyPath> list = serviceDirtyPaths.get(key);
                list.removeAll(selectedService.getValue());

                //An entity can be inserted in more than one spot. if any spot inserts an entity, remove inserts.
//                for(SiebelBusCompEntity insertedEntity:selectedService.getValue().stream().filter(e->e.isInsert()).map(e2->e2.getDirtySiebelBusComp()).collect(toList())){
//                    for (final Iterator<DirtyPath> iterator = list.iterator(); iterator.hasNext(); ) {
//                        DirtyPath next=iterator.next();
//                        if (next.getDirtySiebelBusComp()==insertedEntity&&next.isInsert()) {
//                            iterator.remove();
//                        }
//                    }
//                }
                for (DirtyPath e : selectedService.getValue()) {
                    if (e.isInsert()) {
                        SiebelBusCompEntity insertedEntity = e.getDirtySiebelBusComp();
                        for (final Iterator<DirtyPath> iterator = list.iterator(); iterator.hasNext();) {
                            DirtyPath next = iterator.next();
                            if (next.getDirtySiebelBusComp() == insertedEntity && next.isInsert()) {
                                iterator.remove();
                            }
                        }
                    }
                }

                if (list.size() == 0) {
                    logger.debug("all paths covered, no need to run service:" + key.name);
                    serviceDirtyPaths.remove(key);
                }
            }

        }

        logger.debug("ORDERED SERVICE CALLS:");
        for (Map.Entry<WebService, List<DirtyPath>> e1 : orderedServices) {
            logger.debug("Web Service:" + e1.getKey().name);
            for (DirtyPath dp : e1.getValue()) {
                logger.debug("DirtyPath:" + dp);
            }
        }

        // fire the ordered service calls
        for (Map.Entry<WebService, List<DirtyPath>> selectedService : orderedServices) {
            String webServiceName = selectedService.getKey().name;
            List<DirtyPath> webServiceDirtyPaths = selectedService.getValue();
            this.persistDirtyPaths(slice, webServiceName, webServiceDirtyPaths);
        }
        return true;
    }

    public int closestToServiceRoot(Map.Entry<WebService, List<DirtyPath>> e) {
        int min = Integer.MAX_VALUE;
        String rootBusCompName = e.getKey().repositoryIntegrationObject.getRootBusCompName();
        for (DirtyPath dp : e.getValue()) {
            int rootIdx = dp.bcNames.indexOf(rootBusCompName);
            int wasted = dp.getMaxIndex() - rootIdx;
            if (wasted < min) {
                min = wasted;
            }
        }
        return min;
    }

    public boolean persistDirtyPaths(Slice slice, String webServiceName, List<DirtyPath> dirtyPaths) {
        if (logger.isDebugEnabled()) {

            logger.debug("Building service call to:" + webServiceName);
            logger.debug("Found " + dirtyPaths.size() + " dirty paths to process: ");
            for (DirtyPath dp : dirtyPaths) {
                logger.debug(dp);
            }

        }
        SiebelWebServices.WebService meta = this.WebServiceCache.getWebService(webServiceName);
        processMessage(slice, dirtyPaths, meta);

        // need to hard code the cleanup of the pub master case stuff
        // because of how siebel deals with master cases.
        String associatedCaseLink = SiebelLink.getAssocTagName("PUBMasterCase_HLSCase");
        DLEntity pubMasterCaseEntity = slice.getPubMasterCaseEntity();
        if (pubMasterCaseEntity != null) {
            if (pubMasterCaseEntity.hasCachedLink(associatedCaseLink)) {
                pubMasterCaseEntity.removeChild(associatedCaseLink);
            }
        }
        return true;
    }

    public void processMessage(Slice slice, List<DirtyPath> dirtyPaths, SiebelWebServices.WebService meta) {
        String rootBcName = meta.repositoryIntegrationObject.getRootBusCompName();
        logger.debug("Organize dirty paths as an entity tree rooted with busComps of type [" + rootBcName + "]");
        List<DirtyNode> dirtyNodes = this.createDirtyNodeTree(dirtyPaths, rootBcName);
        for (DirtyNode dirtyNode : dirtyNodes) {
            logger.debug("Dirty Node:" + dirtyNode.toString());
        }
        logger.debug("Setup the web service operation value");
        setWebServiceOperation(dirtyNodes);
        logger.debug("Building soap message");
        SOAPMessage message = meta.buildMessage(dirtyNodes);
        logger.debug("Sending soap message");
        SOAPMessage response = meta.sendMessage(slice, message);
        logger.debug("Processing soap response");
        if (logger.isDebugEnabled()) {
            logger.debug("Response: " + XmlUtility.toPrettyString(response.getSOAPPart()));
        }
        logger.debug("Dirty Nodes: " + dirtyNodes.toString());
        meta.processResponse(response, dirtyNodes);
        logger.debug("Cleanup the dirty nodes");
        this.cleanupDirtyPaths(slice, dirtyPaths);

        DLEntity pubMasterCase = slice.getPubMasterCaseEntity();
        if (pubMasterCase instanceof SiebelBusCompEntity) {
            SiebelBusCompEntity pubMasterCaseSiebel = (SiebelBusCompEntity) pubMasterCase;
            logger.debug("Mark pubMasterCase as persisted");
            pubMasterCaseSiebel.setPersisted();
        }
    }

    public void cleanupDirtyPaths(Slice slice, List<DirtyPath> dirtyPaths) {
        for (DirtyPath dirtyPath : dirtyPaths) {
            logger.debug("Cleanup dirtypath:" + dirtyPath);
            SiebelBusCompEntity entity = dirtyPath.getDirtySiebelBusComp();
            switch (dirtyPath.operation) {
                case Insert:
                    FieldFilter filter = dataLayer.lazyFieldMaster.getFieldFilter(slice, entity.tagName, "Id");
                    for (String fieldName : filter.list) {
                        if (entity.getField(fieldName) == null || entity.getField(fieldName).equals("")) {
                            logger.debug("Adding empty string value for missing field: " + fieldName);
                            entity.setField(fieldName, "");
                        }
                    }
                    entity.setPersisted();
                    break;
                case Update:
                    for (DLField field : entity.getCachedDLFields()) {
                        if (field.hasAttribute("old_value")) {
                            field.removeAttribute("old_value");
                        }
                    }
                    break;
                case Delete: {
                    DLEntity parentEntity = null;
                    String linkAssocName = null;
                    SiebelLinkDisassoc siebelLinkDisAssoc = null;
                    if (dirtyPath.entities.size() > 1) {
                        parentEntity = dirtyPath.entities.get(dirtyPath.entities.size() - 2);
                        linkAssocName = dirtyPath.linkName;
                        siebelLinkDisAssoc = (SiebelLinkDisassoc) parentEntity.getLink(linkAssocName);
                    } else {
                        parentEntity = dirtyPath.getDirtySiebelBusComp().getParentEntity();
                        siebelLinkDisAssoc = (SiebelLinkDisassoc) dirtyPath.getDirtySiebelBusComp().getParentNode();
                        linkAssocName = siebelLinkDisAssoc.getLinkName();
                    }

                    siebelLinkDisAssoc.removeChild(entity);
                    if (siebelLinkDisAssoc.getChildNodes().isEmpty()) {
                        parentEntity.removeChild(linkAssocName);
                    }
                    break;
                }
                case Associate: {
                    DLEntity parentEntity = dirtyPath.entities.get(dirtyPath.entities.size() - 2);
                    String linkAssocName = dirtyPath.linkName;
                    SiebelLinkAssoc siebelLinkAssoc = (SiebelLinkAssoc) parentEntity.getLink(linkAssocName);
                    siebelLinkAssoc.removeChild(entity);
                    if (siebelLinkAssoc.getChildNodes().isEmpty()) {
                        parentEntity.removeChild(linkAssocName);
                    }
                    break;
                }
                //FIXME!!!
                case SetMVG:
                    DLEntity parentEntity = dirtyPath.getParentOfDirtySiebelBusComp();
                    if (parentEntity != null) {
                        if (entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().contains(parentEntity)) {
                            logger.debug("SetMVG cleanup: Cleaning SetMVG parent");
                            entity.getLink("OGDL_PrimaryParentEntities").removeChild(parentEntity);
                        } else {
                            logger.debug("SetMVG cleanup: Parent has been removed already!");
                        }
                    } else {
                        logger.debug("SetMVG cleanup: SetMVG has no Parent entity!");
                    }
                    break;
                case None:
                    break;
                default:
                    throw new AssertionError(dirtyPath.operation.name());
            }
        }
    }

    public List<DirtyPath> getDirtyPaths(List<SiebelBusCompEntity> entities) {
        List<DirtyPath> out = Lists.newArrayList();
        for (SiebelBusCompEntity entity : entities) {
            out.addAll(this.getDirtyPaths(entity));
        }
        return out;
    }

    public List<DirtyPath> getDirtyPaths(SiebelBusCompEntity entity) {
        List<SiebelBusCompEntity> entities = Lists.newArrayList();
        entities.add(entity);
        return getChildDirtyPaths(entities);
    }

    public List<DirtyPath> getChildDirtyPaths(List<SiebelBusCompEntity> entities) {
        List<DirtyPath> out = Lists.newArrayList();
        SiebelBusCompEntity entity = entities.get(entities.size() - 1);
        SiebelBusCompEntity parentEntity = null;
        if (entities.size() > 1) {
            parentEntity = entities.get(entities.size() - 2);
        }

        boolean isDirtyPathSetMVG = entity.getLink("OGDL_PrimaryParentEntities") != null
                && entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().size() > 0
                && parentEntity != null
                && entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().contains(parentEntity);

        if (isDirtyPathSetMVG) {
            DirtyPath dirtyPath = new DirtyPath(entities, DirtyPath.Operation.SetMVG);

            // Check if the entity is already in Siebel
            if (entity.isPersisted()) {
                // If it is already persisted, find the first webservice that can process this dirtyPath
                boolean found = false;
                for (String webServiceName : this.webServiceNames) {
                    WebService webService = this.WebServiceCache.getWebService(webServiceName);
                    if (webService.canProcessPath(dirtyPath)) {
                        // Once found find the multivalue link between the parent entity and the current entity
                        String childBCName = entity.getNativeName();
                        String parentBCName = parentEntity.getNativeName();
                        RepositoryIntegrationComponent ric = webService.repositoryIntegrationObject.getRepositoryIntegrationComponentForBusComp(parentBCName, childBCName, true);
                        String multiValueLink = ric.getMVGLink();
                        BusCompMultiValueLink mvLink = this.dataLayer.siebelBusCompMetaData.siebelBusinessComponentMultiValueLinksCache.getBusCompMultiValueLink(parentBCName, multiValueLink);

                        // Get the primary id field for the multivalue link and see if the current entity id value matches
                        // with the value stored in this primary id field. Only add the dirty path if it matches!
                        String primaryIDFieldName = mvLink.primaryIDFieldName;
                        primaryIDFieldName = StringUtility.toJavaName(primaryIDFieldName);
                        String entityId = entity.getField("Id");
                        String primaryIDOnParentEntity = parentEntity.getField(primaryIDFieldName);
                        if (!entityId.equals(primaryIDOnParentEntity)) {

                            out.add(dirtyPath);
                        }
                        found = true;
                        break;
                    }
                }

                // Just log an error that we cannot find an MVL Link to persist this MVG dirty path.
                if (!found) {
                    logger.error("Error cannot find MVL Link that can persist this MVG dirty path:" + dirtyPath);
                }
            } // If it is not persisted then obviously add it to our dirty Paths
            else {
                out.add(dirtyPath);
            }
        }

        // If our parent is a PUB Master Case, we don't want that entity inserted or updated!!
        if (!entity.getTagName().equals("PUBMasterCase")) {
            if (!entity.isPersisted()) {
                DirtyPath dirtyPath = new DirtyPath(entities, DirtyPath.Operation.Insert);
                out.add(dirtyPath);
            } else {
                if (entity.getTagName().equals("HLSCase") && entity.getParentNode() != null && entity.getParentNode().getTagName().equals("deleted-PUBMasterCase_HLSCase")) {
                    DirtyPath dirtyPath = new DirtyPath(entities, DirtyPath.Operation.Delete);
                    out.add(dirtyPath);
                } else {
                    for (DLField field : entity.getCachedDLFields()) {
                        if (field.hasAttribute("old_value") && !field.getAttribute("old_value").equals("OGSPECIALDLEMPTY") && !field.getAttribute("old_value").equals(field.getFieldValue())) {
                            DirtyPath dirtyPath = new DirtyPath(entities, DirtyPath.Operation.Update);
                            out.add(dirtyPath);
                            continue;
                        }
                    }
                }
            }
        }

        int startingSize = entities.size();
        entities.add(null);
        for (DLLink link : entity.getCachedDLLinks()) {
            //Don't process PUBMasterCase_HLSCase links.
            if (entity.getTagName().equals("PUBMasterCase") && link.getTagName().contains("HLSCase")) {
                continue;
            }

            if (link instanceof SiebelLinkAssoc) {
                for (DLNode childNode : link.getCachedChildNodes()) {
                    SiebelBusCompEntity childEntity = (SiebelBusCompEntity) childNode;
                    entities.set(startingSize, childEntity);
                    DirtyPath dirtyPath = new DirtyPath(entities, DirtyPath.Operation.Associate);
                    dirtyPath.linkName = link.getTagName();
                    out.add(dirtyPath);
                }
            } else if (link instanceof SiebelLinkDisassoc) {
                // new RuntimeException("TBD dissoc link");
                for (DLNode childNode : link.getCachedChildNodes()) {
                    SiebelBusCompEntity childEntity = (SiebelBusCompEntity) childNode;
                    entities.set(startingSize, childEntity);
                    DirtyPath dirtyPath = new DirtyPath(entities, DirtyPath.Operation.Delete);
                    dirtyPath.linkName = link.getTagName();
                    out.add(dirtyPath);
                    List<DirtyPath> dirtyPaths = getChildDirtyPaths(entities);
                    out.addAll(dirtyPaths);
                }
            } else if (link instanceof SiebelLink) {
                for (DLNode childNode : link.getCachedChildNodes()) {
                    SiebelBusCompEntity childEntity = (SiebelBusCompEntity) childNode;
                    entities.set(startingSize, childEntity);
                    List<DirtyPath> dirtyPaths = getChildDirtyPaths(entities);
                    out.addAll(dirtyPaths);
                }
            }
        }
        entities.remove(startingSize);
        return out;
    }

    public static class DirtyPath {

        public enum Operation {

            Insert, Update, Delete, Associate, SetMVG, None
        };
        public Operation operation;
        public List<String> bcNames = Lists.newArrayList();
        public List<SiebelBusCompEntity> entities = Lists.newArrayList();
        // if operation is assoc or disassoc this needs to be set.
        public String linkName;

        public static Map<String, List<DirtyPath>> groupByRootSiebelBusCompName(List<DirtyPath> input) {
            Map<String, List<DirtyPath>> out = new HashMap<>();
            for (DirtyPath dirtyPath : input) {
                CollectionUtility.append(out, dirtyPath.getRootSiebelBusCompName(), dirtyPath);
            }
            return out;
        }

        public static Map<SiebelBusCompEntity, List<DirtyPath>> groupByRootSiebelBusComp(List<DirtyPath> input) {
            Map<SiebelBusCompEntity, List<DirtyPath>> out = new HashMap<>();
            for (DirtyPath dirtyPath : input) {
                CollectionUtility.append(out, dirtyPath.getRootSiebelBusComp(), dirtyPath);
            }
            return out;
        }

        public DirtyPath(List<SiebelBusCompEntity> entities, Operation operation) {
            this.entities = Lists.newArrayList(entities); // need to copy 
            this.operation = operation;
            for (SiebelBusCompEntity entity : this.entities) {
                this.bcNames.add(entity.getNativeName());
            }
        }

        public DirtyPath(DirtyPath copy, int rootIdx) {
            this(copy.entities.subList(rootIdx, copy.entities.size()), copy.operation);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("operation", operation)
                    .add("bcNames", bcNames)
                    .toString();
        }

        public int getMaxIndex() {
            return this.bcNames.size() - 1;
        }

        public SiebelBusCompEntity getDirtySiebelBusComp() {
            return this.entities.get(this.getMaxIndex());
        }

        public SiebelBusCompEntity getParentOfDirtySiebelBusComp() {
            if (this.getMaxIndex() > 0) {
                return this.entities.get(this.getMaxIndex() - 1);
            }
            return null;
        }

        public SiebelBusCompEntity getGrandParentOfDirtySiebelBusComp() {
            if (this.getMaxIndex() > 1) {
                return this.entities.get(this.getMaxIndex() - 2);
            }
            return null;
        }

        public SiebelBusCompEntity getGreatGrandParentOfDirtySiebelBusComp() {
            if (this.getMaxIndex() > 2) {
                return this.entities.get(this.getMaxIndex() - 3);
            }
            return null;
        }

        public SiebelBusCompEntity getGreatGreatGrandParentOfDirtySiebelBusComp() {
            if (this.getMaxIndex() > 3) {
                return this.entities.get(this.getMaxIndex() - 4);
            }
            return null;
        }

        public SiebelBusCompEntity getRootSiebelBusComp() {
            return this.entities.get(0);
        }

        public String getDirtySiebelBusCompName() {
            return this.bcNames.get(this.getMaxIndex());
        }

        public String getRootSiebelBusCompName() {
            return this.bcNames.get(0);
        }

        public boolean isInsert() {
            return this.operation == Operation.Insert;
        }

        public boolean isUpdate() {
            return this.operation == Operation.Update;
        }

        public boolean isDelete() {
            return this.operation == Operation.Delete;
        }

        public boolean isSetMVG() {
            return this.operation == Operation.SetMVG;
        }

        public boolean isAssociate() {
            return this.operation == Operation.Associate;
        }
    }

    public void setWebServiceOperation(List<DirtyNode> dirtyNodes) {
        for (DirtyNode dirtyNode : dirtyNodes) {
            dirtyNode.operationAttributeValue = "upsert";
        }
    }

    public void assignDummyIds(List<DirtyPath> dirtyPaths) {
        int dummyId = 1;
        for (DirtyPath dirtyPath : dirtyPaths) {
            if (dirtyPath.operation == DirtyPath.Operation.Insert) {
                DLEntity entity = dirtyPath.entities.get(dirtyPath.entities.size() - 1);
                String idValue = entity.getField("Id");
                if (StringUtility.isEmpty(idValue)) {
                    entity.setField("Id", String.valueOf(dummyId++));
                }
            }
        }
    }

    public List<DirtyPath> rerootDirtyPaths(List<DirtyPath> dirtyPaths, String webServiceRootBusCompName) {
        List<DirtyPath> rerootedPaths = new ArrayList<>();
        for (DirtyPath dirtyPath : dirtyPaths) {
            int rootIdx = dirtyPath.bcNames.lastIndexOf(webServiceRootBusCompName);
            DirtyPath rerootedPath = new DirtyPath(dirtyPath, rootIdx);
            rerootedPaths.add(rerootedPath);
        }
        return rerootedPaths;
    }

    // the tree that we need to build has a list of roots e
    // returns a list of all one type.
    public List<DirtyNode> createDirtyNodeTree(List<DirtyPath> dirtyPaths, String webServiceRootBusCompName) {
        List<DirtyPath> rerootedDirtyPaths = this.rerootDirtyPaths(dirtyPaths, webServiceRootBusCompName);
        List<DirtyNode> out = new ArrayList<>();
        Map<SiebelBusCompEntity, List<DirtyPath>> groupedDirtyPaths = DirtyPath.groupByRootSiebelBusComp(rerootedDirtyPaths);
        for (List<DirtyPath> group : groupedDirtyPaths.values()) {
            DirtyNode n = new DirtyNode(group.get(0).getRootSiebelBusComp(), group);
            out.add(n);
        }
        return out;
    }

    // consolidates N dirty paths into a single tree
    public static class DirtyNode {

        SiebelBusCompEntity entity;
        String busCompName;
        List<DirtyPath> dirtyPaths;
        List<DirtyPath.Operation> operations = Lists.newArrayList();

        String operationAttributeValue;
        DirtyNode parentDirtyNode;
        List<DirtyNode> parentDirtyNodeGroup;
        List<List<DirtyNode>> groupedchildNodes = Lists.newArrayList();

        public DirtyNode(SiebelBusCompEntity entity, List<DirtyPath> dirtyPaths) {
            this.entity = entity;
            this.busCompName = entity.getNativeName();
            this.dirtyPaths = dirtyPaths;

            // set operations on terminal paths and create new child paths.
            List<DirtyPath> childPaths = new ArrayList<>();
            for (DirtyPath dirtyPath : dirtyPaths) {
                if (dirtyPath.getMaxIndex() == 0) {
                    this.operations.add(dirtyPath.operation);
                } else {
                    childPaths.add(new DirtyPath(dirtyPath, 1));
                }
            }
            // group the dirty paths by child BC Name
            // then group by child entity
            Map<String, List<DirtyPath>> bcNamePathMap = DirtyPath.groupByRootSiebelBusCompName(childPaths);
            for (List<DirtyPath> bcNameList : bcNamePathMap.values()) {
                List<DirtyNode> nodesByBcName = new ArrayList<>();
                Map<SiebelBusCompEntity, List<DirtyPath>> bcInstancePathMap = DirtyPath.groupByRootSiebelBusComp(bcNameList);
                for (List<DirtyPath> bcInstanceList : bcInstancePathMap.values()) {
                    DirtyNode dirtyNode = new DirtyNode(bcInstanceList.get(0).entities.get(0), bcInstanceList);
                    dirtyNode.parentDirtyNode = this;
                    dirtyNode.parentDirtyNodeGroup = nodesByBcName;
                    nodesByBcName.add(dirtyNode);
                }
                this.groupedchildNodes.add(nodesByBcName);
            }

//            if(this.groupedchildNodes.size()==0){
//                logger.debug("No child nodes here..");
//            }
        }

        public DirtyNode getParentDirtyNode() {
            return parentDirtyNode;
        }

        public List<DirtyNode> getParentDirtyNodeGroup() {
            return parentDirtyNodeGroup;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("<DirtyNode bc='" + this.busCompName + "'>" + StringUtility.NL);
            if (this.operations.size() > 0) {
                for (Operation op : this.operations) {
                    sb.append("<Operation>" + op + "</Operation>" + StringUtility.NL);
                }
            }
            if (this.groupedchildNodes.size() > 0) {
                for (List<DirtyNode> group : this.groupedchildNodes) {
                    sb.append("<ChildNodes group='" + group.get(0).busCompName + "'>" + StringUtility.NL);
                    for (DirtyNode n : group) {
                        sb.append(n.toString() + StringUtility.NL);
                    }
                    sb.append("</ChildNodes>" + StringUtility.NL);
                }

            }
            sb.append("</DirtyNode>" + StringUtility.NL);
            return sb.toString();
        }
    }

    public static class WebService {

        Slice staticSlice;
        String name;
        String namespace;
        String status;
        List<Port> listOfPort = Lists.newArrayList();
        String integrationObjectName;
        RepositoryIntegrationObject repositoryIntegrationObject;

        public static class Port {

            WebService webService;
            String implementationName;
            String implementionType;
            String name;
            String address;
            List<WorkFlowProcess> listOfWorkFlowProcess = Lists.newArrayList();

            public static class WorkFlowProcess {

                Port port;
                String processName;
                String status;
                List<WorkFlowProcessProps> ListOfWorkFlowProcessProps = Lists.newArrayList();

                public static class WorkFlowProcessProps {

                    WorkFlowProcess workFlowProcess;
                    String integrationObject;
                }
            }
        }

        public static class RepositoryIntegrationObject {

            WebService webService;
            String name;
            String externalName;
            String xmlTag;
            List<RepositoryIntegrationComponent> listOfRepositoryIntegrationComponent = Lists.newArrayList();

            public static class RepositoryIntegrationComponent {

                RepositoryIntegrationObject repositoryIntegrationObject;
                String name;
                String externalName;
                String xmlTag;
                String parentIntegrationComponent;
                String XmlContainerElement;
                List<RepositoryIntegrationComponentField> listOfRepositoryIntegrationComponentField = Lists.newArrayList();
                List<RepositoryIntegrationComponentKey> listOfRepositoryIntegrationComponentKey = Lists.newArrayList();
                List<RepositoryIntegrationComponentUserProp> listOfRepositoryIntegrationComponentUserProp = Lists.newArrayList();

                public String getMVGLink() {
                    for (RepositoryIntegrationComponentUserProp prop : listOfRepositoryIntegrationComponentUserProp) {
                        if (prop.name.equals("MVGLink")) {
                            return prop.value;
                        }
                    }
                    return null;
                }

                public boolean isMVG() {
                    for (RepositoryIntegrationComponentUserProp prop : listOfRepositoryIntegrationComponentUserProp) {
                        if (prop.name.equals("MVGAssociation") && prop.value.equals("Y")) {
                            return true;
                        }
                    }
                    return false;
                }

                public RepositoryIntegrationComponentKey getUserKey() {
                    for (RepositoryIntegrationComponentKey rick : this.listOfRepositoryIntegrationComponentKey) {
                        if (rick.keyType.equals("User Key")) {
                            return rick;
                        }
                    }
                    throw new RuntimeException("User Key not found for integration component [" + this.name + "]");
                }

                public RepositoryIntegrationComponentKey getStatusKey() {
                    for (RepositoryIntegrationComponentKey rick : this.listOfRepositoryIntegrationComponentKey) {
                        if (rick.keyType.equals("Status Key")) {
                            return rick;
                        }
                    }
                    throw new RuntimeException("Status Key not found for integration component [" + this.name + "]");
                }

                public List<RepositoryIntegrationComponentField> getStatusKeyFields() {
                    List<RepositoryIntegrationComponentField> out = Lists.newArrayList();
                    RepositoryIntegrationComponentKey rick = this.getStatusKey();
                    if (rick != null) {
                        RepositoryIntegrationComponentField idRicf = null;
                        for (RepositoryIntegrationComponentKeyField rickf : rick.listOfRepositoryIntegrationComponentKeyField) {
                            RepositoryIntegrationComponentField ricf = this.getRepositoryIntegrationComponentField(rickf.fieldName);
                            if (ricf.xmlTag.equals("Id")) {
                                idRicf = ricf;
                            } else {
                                out.add(ricf);
                            }
                        }
                        // Add Id field to the beginning if it exists!
                        if (idRicf != null) {
                            out.add(0, idRicf);
                        }
                    }
                    return out;
                }

                public List<String> getUserKeyBusCompFieldNames() {
                    List<String> busCompKeyFieldNames = Lists.newArrayList();
                    RepositoryIntegrationComponentKey rick = this.getUserKey();
                    if (rick != null) {
                        for (RepositoryIntegrationComponentKeyField rickf : rick.listOfRepositoryIntegrationComponentKeyField) {
                            RepositoryIntegrationComponentField ricf = this.getRepositoryIntegrationComponentField(rickf.fieldName);
                            busCompKeyFieldNames.add(ricf.getBusCompFieldName());
                        }
                    }
                    //added to make "PUB Case Benefit Plan" upsert work properly. 
                    //Without this,""Program Name" is a required field.  Please enter a value for the field.(SBL-DAT-00498)(SBL-EAI-04389)" error is encountered. 
                    //Tried to use all the UserKeys in RIC but lot of test cases failed. 
                    //TODO: Needs to be fixed properly
                    if (this.externalName.equalsIgnoreCase("PUB Case Benefit Plan")) {
                        busCompKeyFieldNames.add("Program Name");
                    }
                    return busCompKeyFieldNames;
                }

                public List<RepositoryIntegrationComponentField> getUserKeyRepositoryIntegrationComponentFields() {
                    List<RepositoryIntegrationComponentField> out = Lists.newArrayList();
                    RepositoryIntegrationComponentKey rick = this.getUserKey();
                    if (rick != null) {
                        for (RepositoryIntegrationComponentKeyField rickf : rick.listOfRepositoryIntegrationComponentKeyField) {
                            RepositoryIntegrationComponentField ricf = this.getRepositoryIntegrationComponentField(rickf.fieldName);
                            out.add(ricf);
                        }
                    }
                    return out;
                }

                public String getBusCompName() {
                    return this.externalName;
                }

                public String getListOfTagName() {
                    if (notEmpty(this.XmlContainerElement)) {
                        return this.XmlContainerElement;
                    }
                    return this.repositoryIntegrationObject.xmlTag;
                }

                public RepositoryIntegrationComponentField getRepositoryIntegrationComponentFieldForBusCompField(String busCompFieldName) {
                    for (RepositoryIntegrationComponentField ricf : this.listOfRepositoryIntegrationComponentField) {
                        if (ricf.getBusCompFieldName().equals(busCompFieldName)) {
                            return ricf;
                        }
                    }
                    return null;
                    //throw new RuntimeException("Cannot find RepositoryIntegrationComponentField with externalName=["+busCompFieldName+"] for integration component ["+this.name+"]");
                }

                public RepositoryIntegrationComponentField getRepositoryIntegrationComponentField(String name) {
                    for (RepositoryIntegrationComponentField ricf : this.listOfRepositoryIntegrationComponentField) {
                        if (ricf.name.equals(name)) {
                            return ricf;
                        }
                    }
                    String err = "Cannot find RepositoryIntegrationComponentField with name=[" + name + "] for integration component [" + this.name + "]";
                    logger.error(err);
                    throw new RuntimeException(err);
                }

                public static class RepositoryIntegrationComponentUserProp {

                    RepositoryIntegrationComponent repositoryIntegrationComponent;
                    String name;
                    String value;
                }

                public static class RepositoryIntegrationComponentKey implements Comparable {

                    RepositoryIntegrationComponent repositoryIntegrationComponent;
                    String name;
                    String keyType;
                    int order;
                    List<RepositoryIntegrationComponentKeyField> listOfRepositoryIntegrationComponentKeyField = Lists.newArrayList();

                    @Override
                    public int compareTo(Object o) {
                        if (!(o instanceof RepositoryIntegrationComponentKey)) {
                            throw new RuntimeException("Can't compare with a non RepositoryIntegrationComponentKey object");
                        }
                        RepositoryIntegrationComponentKey that = (RepositoryIntegrationComponentKey) o;

                        if (this.order > that.order) {
                            return 1;
                        }
                        if (this.order < that.order) {
                            return -1;
                        }
                        return this.name.compareTo(that.name);

                    }

                    public static class RepositoryIntegrationComponentKeyField {

                        RepositoryIntegrationComponentKey repositoryIntegrationComponentKey;
                        String name;
                        String fieldName;
                    }

                }

                public static class RepositoryIntegrationComponentField {

                    RepositoryIntegrationComponent repositoryIntegrationComponent;
                    String externalName;
                    String xmlTag;
                    String name;
                    String externalDataType;

                    public String getBusCompFieldName() {
                        Slice slice = this.repositoryIntegrationComponent.repositoryIntegrationObject.webService.staticSlice;
                        RepositoryIntegrationComponent ric = this.repositoryIntegrationComponent;
                        if (ric.isMVG()) {
                            String multiValueLink = ric.getMVGLink();
                            String parentRicName = ric.parentIntegrationComponent;
                            RepositoryIntegrationComponent parentRic = ric.repositoryIntegrationObject.getRepositoryIntegrationComponentForName(parentRicName);
                            String parentBcName = parentRic.getBusCompName();
                            BusCompMultiValueLinkFields fields = slice.getDataLayer().siebelBusCompMetaData.busCompMultiValueLinkFieldsCache.getBusCompMultiValueLinkFields(parentBcName, multiValueLink);
                            for (BusCompMultiValueLinkFields.BusCompMultiValueLinkField mvgField : fields.rows) {
                                if (mvgField.name.equals(this.name)) {
                                    return mvgField.field;
                                }
                            }
                        }
                        return this.externalName;
                    }
                }

                public RepositoryIntegrationComponent getChildRepositoryIntegrationComponentForBusComp(String busCompName, boolean getMVG) {
                    return this.repositoryIntegrationObject.getRepositoryIntegrationComponentForBusComp(this.name, busCompName, getMVG);
                }
            }

            public List<RepositoryIntegrationComponent> getRepositoryIntegrationComponentWithParentName(String parentName) {
                List<RepositoryIntegrationComponent> out = Lists.newArrayList();
                for (RepositoryIntegrationComponent ric : this.listOfRepositoryIntegrationComponent) {
                    if (ric.parentIntegrationComponent.equals(parentName)) {
                        out.add(ric);
                    }
                }
                return out;
            }

            public RepositoryIntegrationComponent getRepositoryIntegrationComponentForBusComp(String parentName, String busCompName, boolean getMVG) {
                List<RepositoryIntegrationComponent> childRics = this.getRepositoryIntegrationComponentWithParentName(parentName);

                List<RepositoryIntegrationComponent> possible = new ArrayList<>();
                // If person did not specify that he wants an MVG, select non-mvg first
                if (!getMVG) {
                    for (RepositoryIntegrationComponent a : childRics) {
                        if (!a.isMVG() && a.getBusCompName().equals(busCompName)) {
                            possible.add(a);
                        }
                    }
                }
//                List<RepositoryIntegrationComponent> possible =
//                         childRics.stream()
//                         .filter(a->(!a.isMVG())&&a.getBusCompName().equals(busCompName)).collect(toList());

                if (possible.size() == 1) {
                    logger.debug("Returning non-mvg RIC " + possible.get(0).name);
                    return possible.get(0);
                } else if (possible.size() > 1) {
                    List<String> list = new ArrayList<>();
                    for (RepositoryIntegrationComponent item : possible) {
                        list.add(item.name);
                    }
                    throw new RuntimeException("Cannot choose between these integration components: " + Joiner.on(",").join(list));
                }

                // Otherwise try mvg 
                for (RepositoryIntegrationComponent a : childRics) {
                    if (a.isMVG() && a.getBusCompName().equals(busCompName)) {
                        possible.add(a);
                    }
                }

                if (possible.size() == 1) {
                    logger.debug("Returning mvg RIC " + possible.get(0).name);
                    return possible.get(0);
                } else if (possible.size() > 1) {
                    List<String> list = new ArrayList<>();
                    for (RepositoryIntegrationComponent p : possible) {
                        list.add(p.name);
                    }
                    throw new RuntimeException("Cannot choose between these integration components: " + Joiner.on(",").join(list));
                }

                return null;
                //throw new RuntimeException("Cannot find an integration components for busCompName ["+busCompName+"] with parent integration component name ["+parentName+"]");
            }

            public RepositoryIntegrationComponent getRepositoryIntegrationComponentForTagName(String tagName) {
                for (RepositoryIntegrationComponent ric : this.listOfRepositoryIntegrationComponent) {
                    if (ric.xmlTag.equals(tagName)) {
                        return ric;
                    }
                }
                throw new RuntimeException("Cannot find RepositoryIntegrationComponent with tagName=[" + tagName + "]");
            }

            public RepositoryIntegrationComponent getRepositoryIntegrationComponentForName(String name) {
                for (RepositoryIntegrationComponent ric : this.listOfRepositoryIntegrationComponent) {
                    if (ric.name.equals(name)) {
                        return ric;
                    }
                }
                throw new RuntimeException("Cannot find RepositoryIntegrationComponent with name=[" + name + "]");
            }

            public String getRootBusCompName() {
                return this.externalName;
            }

            public RepositoryIntegrationComponent getRootRepositoryIntegrationComponent(boolean setMVG) {
                String busCompName = this.getRootBusCompName();

                List<RepositoryIntegrationComponent> possible = new ArrayList<>();
                for (RepositoryIntegrationComponent a : this.listOfRepositoryIntegrationComponent) {
                    if (a.getBusCompName().equals(busCompName) && a.parentIntegrationComponent.equals("")) {
                        if (!setMVG) {
                            possible.add(a);
                        } else if (setMVG && a.isMVG()) {
                            possible.add(a);
                        }

                    }
                }
//                List<RepositoryIntegrationComponent> possible =
//                         this.listOfRepositoryIntegrationComponent.stream()
//                         .filter(a->(a.getBusCompName().equals(busCompName))).collect(toList());
                if (possible.size() == 1) {
                    logger.debug("Returning root RIC " + possible.get(0).name);
                    return possible.get(0);
                } else if (possible.size() > 1) {
                    List<String> list = new ArrayList<>();

                    logger.error("Expecting to match only 1 root repository integration component for web service [" + name + "] but found [" + possible.size() + "] that match on [" + busCompName + "]!");
                    for (RepositoryIntegrationComponent item : possible) {
                        logger.error("Matched:" + item.name);
                        list.add(item.name);
                    }
                    logger.error("All repository integration components:");
                    for (RepositoryIntegrationComponent a : this.listOfRepositoryIntegrationComponent) {
                        logger.error("ALL:" + a.name);
                    }
                    throw new RuntimeException("Error with webService [" + this.name + "] Cannot choose between these ROOT integration components: (" + Joiner.on(",").join(list) + "), setMVG=" + setMVG);
                }

                throw new RuntimeException("Cannot find RepositoryIntegrationComponent for root busCompName=[" + busCompName + "]");
            }

        }

        public WebService(Slice staticSlice, String webServiceName) {
            logger.debug("Constructing WebService-" + webServiceName);
            this.staticSlice = staticSlice;
            this.name = webServiceName;
            String repositoryId = staticSlice.dataLayer.siebelRepositoryCache.getRepositoryId();
            String webServiceBC = "Web Service";
            Map<String, String> webServiceParams = Maps.newHashMap();
            webServiceParams.put("Name", webServiceName);

            for (DLEntity wsEntity : staticSlice.dataLayer.staticEntityLookupCache.getSiebelEntity(webServiceBC, webServiceParams)) {
                //logger.debug("WS.Name="+wsEntity.getField("Name"));
                //logger.debug("WS.Namepace="+wsEntity.getField("Namespace"));
                //logger.debug("WS.Status="+wsEntity.getField("Status"));
                this.name = wsEntity.getField("Name");
                this.status = wsEntity.getField("Status");
                this.namespace = wsEntity.getField("Namespace");
                for (DLEntity portEntity : wsEntity.getChildEntities("WebService_Port")) {
                    // logger.debug("PORT.Implementation Name="+portEntity.getField("ImplementationName"));
                    //logger.debug("PORT.Implementation Type="+portEntity.getField("ImplementationType"));
                    //logger.debug("PORT.Name="+portEntity.getField("Name"));
                    //logger.debug("PORT.Addess="+portEntity.getField("Address"));
                    Port port = new Port();
                    this.listOfPort.add(port);
                    port.webService = this;
                    port.implementationName = portEntity.getField("ImplementationName");
                    port.implementionType = portEntity.getField("ImplementationType");
                    port.name = portEntity.getField("Name");
                    port.address = portEntity.getField("Address");
                    if (port.implementionType.equals("WORKFLOW")) {
                        String workFlowProcessBC = "Repository Workflow Process";
                        Map<String, String> workFlowProcessParams = Maps.newHashMap();
                        workFlowProcessParams.put("Process Name", port.implementationName);
                        workFlowProcessParams.put("Status", "COMPLETED");
                        workFlowProcessParams.put("Repository Id", repositoryId);
                        //logger.debug("Looking up workflow process entity ["+port.implementationName+"]");
                        for (DLEntity wfpEntity : staticSlice.dataLayer.staticEntityLookupCache.getSiebelEntity(workFlowProcessBC, workFlowProcessParams)) {
                            // logger.debug("WFP.ProcessName="+wfpEntity.getField("ProcessName"));
                            //logger.debug("WFP.Status="+wfpEntity.getField("Status"));
                            WorkFlowProcess workFlowProcess = new WorkFlowProcess();
                            port.listOfWorkFlowProcess.add(workFlowProcess);
                            workFlowProcess.port = port;
                            workFlowProcess.processName = wfpEntity.getField("ProcessName");
                            workFlowProcess.status = wfpEntity.getField("Status");
                            for (DLEntity wfppEntity : wfpEntity.getChildEntities("RepositoryWorkflowProcess_RepositoryWFProcessProp")) {
                                //logger.debug("checking a property..");
                                if (notEmpty(wfppEntity.getField("IntegrationObject"))) {
                                    this.integrationObjectName = wfppEntity.getField("IntegrationObject");
                                    WorkFlowProcessProps workFlowProcessProps = new WorkFlowProcessProps();
                                    workFlowProcess.ListOfWorkFlowProcessProps.add(workFlowProcessProps);
                                    workFlowProcessProps.workFlowProcess = workFlowProcess;
                                    workFlowProcessProps.integrationObject = wfppEntity.getField("IntegrationObject");
                                    //logger.debug("**** integrationObjectName="+integrationObjectName);
                                }
                            }
                        }
                    }
                }
            }
            if (this.integrationObjectName == null) {
                throw new RuntimeException("Could not locate an integration object for siebel web service [" + this.name + "]");
            }
            String integrationObjectBC = "Repository Integration Object";
            Map<String, String> integrationObjectParams = Maps.newHashMap();
            integrationObjectParams.put("Name", integrationObjectName);
            integrationObjectParams.put("Repository Id", repositoryId);
            List<DLEntity> ioEntities = staticSlice.dataLayer.staticEntityLookupCache.getSiebelEntity(integrationObjectBC, integrationObjectParams);
            if (ioEntities.size() == 0) {
                throw new RuntimeException("Could not locate an integration object named [" + integrationObjectName + "] for siebel web service [" + this.name + "]");
            }
            if (ioEntities.size() > 1) {
                throw new RuntimeException("Found more than integration object named [" + integrationObjectName + "] for siebel web service [" + this.name + "]");
            }
            DLEntity ioEntity = ioEntities.get(0);
            //logger.debug("RepositoryIntegrationObject.Name="+ioEntity.getField("Name"));
            //logger.debug("RepositoryIntegrationObject.ExternalName="+ioEntity.getField("ExternalName"));
            //logger.debug("RepositoryIntegrationObject.XmlTag="+ioEntity.getField("XMLTag"));
            this.repositoryIntegrationObject = new RepositoryIntegrationObject();
            repositoryIntegrationObject.webService = this;
            repositoryIntegrationObject.name = ioEntity.getField("Name");
            repositoryIntegrationObject.externalName = ioEntity.getField("ExternalName");
            repositoryIntegrationObject.xmlTag = ioEntity.getField("XMLTag");
            DLLink link = ioEntity.getLink("RepositoryIntegrationObject_RepositoryIntegrationComponent");
            int i = 1;
            for (DLNode icNode : link.getChildNodes()) {
                DLEntity icEntity = (DLEntity) icNode;
                if (icEntity.getField("Inactive").equals("N")) {
                    //logger.debug((i++)+":");
                    //logger.debug("  RepositoryIntegrationComponent.Name="+icEntity.getField("Name"));// name to search for to get fields
                    //logger.debug("  RepositoryIntegrationComponent.ParentIntegrationComponent="+icEntity.getField("ParentIntegrationComponent")); // ;
                    //logger.debug("	 RepositoryIntegrationComponent.ExternalName="+icEntity.getField("ExternalName")); // buscomp;
                    //logger.debug("	 RepositoryIntegrationComponent.XmlContainerElement="+icEntity.getField("XMLContainerElement")); // listofBlah
                    //logger.debug("	 RepositoryIntegrationComponent.XmlTag="+icEntity.getField("XMLTag"));// Blah
                    RepositoryIntegrationComponent repositoryIntegrationComponent = new RepositoryIntegrationComponent();
                    repositoryIntegrationObject.listOfRepositoryIntegrationComponent.add(repositoryIntegrationComponent);
                    repositoryIntegrationComponent.repositoryIntegrationObject = repositoryIntegrationObject;
                    repositoryIntegrationComponent.name = icEntity.getField("Name");// name to search for to get fields
                    repositoryIntegrationComponent.parentIntegrationComponent = icEntity.getField("ParentIntegrationComponent");// name to search for to get fields
                    repositoryIntegrationComponent.externalName = icEntity.getField("ExternalName");// name to search for to get fields
                    repositoryIntegrationComponent.XmlContainerElement = icEntity.getField("XMLContainerElement");// name to search for to get fields
                    repositoryIntegrationComponent.xmlTag = icEntity.getField("XMLTag");// name to search for to get fields

                    //logger.debug("	 RepositoryIntegrationComponent.Inactive="+icEntity.getField("Inactive"));// N
                    for (DLEntity icfEntity : icEntity.getChildEntities("RepositoryIntegrationComponent_RepositoryIntegrationComponentUserProp")) {
                        if (icfEntity.getField("Inactive").equals("N")) {
                            //logger.debug("    RepositoryIntegrationComponentUserProp.Name="+icfEntity.getField("Name"));
                            //logger.debug("    RepositoryIntegrationComponentUserProp.Value="+icfEntity.getField("Value"));
                            RepositoryIntegrationComponentUserProp repositoryIntegrationComponentUserProp = new RepositoryIntegrationComponentUserProp();
                            repositoryIntegrationComponent.listOfRepositoryIntegrationComponentUserProp.add(repositoryIntegrationComponentUserProp);
                            repositoryIntegrationComponentUserProp.repositoryIntegrationComponent = repositoryIntegrationComponent;
                            repositoryIntegrationComponentUserProp.value = icfEntity.getField("Value");
                            repositoryIntegrationComponentUserProp.name = icfEntity.getField("Name");
                        }
                    }

                    int f = 1;
                    for (DLEntity icfEntity : icEntity.getChildEntities("RepositoryIntegrationComponent_RepositoryIntegrationComponentField")) {
                        if (icfEntity.getField("Inactive").equals("N")) {
                            //logger.debug("  "+(f++)+":");
                            //logger.debug("    RepositoryIntegrationComponentField.Name="+icfEntity.getField("ExternalName"));// BusComp field name
                            //logger.debug("    RepositoryIntegrationComponentField.XmlTag="+icfEntity.getField("XMLTag"));
                            RepositoryIntegrationComponentField repositoryIntegrationComponentField = new RepositoryIntegrationComponentField();
                            repositoryIntegrationComponent.listOfRepositoryIntegrationComponentField.add(repositoryIntegrationComponentField);
                            repositoryIntegrationComponentField.repositoryIntegrationComponent = repositoryIntegrationComponent;
                            repositoryIntegrationComponentField.externalName = icfEntity.getField("ExternalName");// BusComp field name
                            repositoryIntegrationComponentField.xmlTag = icfEntity.getField("XMLTag");
                            repositoryIntegrationComponentField.name = icfEntity.getField("Name");
                            repositoryIntegrationComponentField.externalDataType = icfEntity.getField("ExternalDataType");
                        }
                    }
                    //Repository Integration Component/Repository Integration Component Key
                    int k = 1;
                    
                    for (DLEntity ickEntity : icEntity.getChildEntities("RepositoryIntegrationComponent_RepositoryIntegrationComponentKey")) {
                        if (ickEntity.getField("Inactive").equals("N")) { //
                            //logger.debug("  ["+repositoryIntegrationComponent.name+"][Key]"+(k++)+":");
                            //logger.debug("    RepositoryIntegrationComponentKey.Name="+ickEntity.getField("Name"));
                            //logger.debug("    RepositoryIntegrationComponentKey.KeyType="+ickEntity.getField("KeyType"));
                            RepositoryIntegrationComponentKey repositoryIntegrationComponentKey = new RepositoryIntegrationComponentKey();
                            repositoryIntegrationComponent.listOfRepositoryIntegrationComponentKey.add(repositoryIntegrationComponentKey);
                            repositoryIntegrationComponentKey.repositoryIntegrationComponent = repositoryIntegrationComponent;
                            repositoryIntegrationComponentKey.name = ickEntity.getField("Name");
                            repositoryIntegrationComponentKey.keyType = ickEntity.getField("KeyType");
                            repositoryIntegrationComponentKey.order = Integer.parseInt(ickEntity.getField("KeySequenceNumber"));
                            //Repository Integration Component Key/Repository Integration Component Key Field
                            for (DLEntity ickfEntity : ickEntity.getChildEntities("RepositoryIntegrationComponentKey_RepositoryIntegrationComponentKeyField")) {
                                int kf = 1;
                                if (ickfEntity.getField("Inactive").equals("N")) {
                                    //logger.debug("  ["+repositoryIntegrationComponent.name+"]["+repositoryIntegrationComponentKey.name+"][KeyField]"+(kf++)+":");
                                    //logger.debug("    RepositoryIntegrationComponentKeyField.Name="+ickfEntity.getField("Name"));
                                    //logger.debug("    RepositoryIntegrationComponentKeyField.FieldName="+ickfEntity.getField("FieldName"));
                                    RepositoryIntegrationComponentKeyField repositoryIntegrationComponentKeyField = new RepositoryIntegrationComponentKeyField();
                                    repositoryIntegrationComponentKey.listOfRepositoryIntegrationComponentKeyField.add(repositoryIntegrationComponentKeyField);
                                    repositoryIntegrationComponentKeyField.repositoryIntegrationComponentKey = repositoryIntegrationComponentKey;
                                    repositoryIntegrationComponentKeyField.name = ickfEntity.getField("Name");
                                    repositoryIntegrationComponentKeyField.fieldName = ickfEntity.getField("FieldName");
                                }
                            }
                        }
                    }

                    Collections.sort(repositoryIntegrationComponent.listOfRepositoryIntegrationComponentKey);
                }
                //logger.debug(ioEntity.toXmlString());
            }
            //logger.debug("done!");
            logger.debug("Constructing WebService-" + webServiceName + " DONE!");
        }

        private Dispatch<SOAPMessage> dispatch;

        private Dispatch<SOAPMessage> getDispatch() {
            if (this.dispatch != null) {
                return this.dispatch;
            }
            synchronized (this) {
                if (dispatch == null) {
                    try (Timer a = staticSlice.getTimer("WebService." + this.name + ".OneTimeSetup")) {
                        String endpoint = this.listOfPort.get(0).address;
                        String targetNameSpaceFromWsdlFile = this.namespace;
                        String serviceNameAttributeFromBottomOfWsdlFile = this.name.replace(" ", "_spc");
                        QName serviceQName = new QName(targetNameSpaceFromWsdlFile, serviceNameAttributeFromBottomOfWsdlFile);
                        Service service = Service.create(serviceQName);
                        String portNameAttributeFromBottomOfWsdlFile = this.listOfPort.get(0).name.replace(" ", "_spc");
                        QName portName = new QName(targetNameSpaceFromWsdlFile, portNameAttributeFromBottomOfWsdlFile);
                        service.addPort(portName, SOAPBinding.SOAP11HTTP_BINDING, endpoint);
                        dispatch = service.createDispatch(portName, SOAPMessage.class, Service.Mode.MESSAGE);
                        String soapAction = "document" + "/" + targetNameSpaceFromWsdlFile + ":" + serviceNameAttributeFromBottomOfWsdlFile;
                        dispatch.getRequestContext().put(javax.xml.ws.BindingProvider.SOAPACTION_URI_PROPERTY, soapAction);
                        dispatch.getRequestContext().put(javax.xml.ws.BindingProvider.SOAPACTION_USE_PROPERTY, Boolean.TRUE);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return this.dispatch;
            }
        }

        public SOAPMessage sendMessage(Slice slice, SOAPMessage request) {
            //Dispatch<SOAPMessage> dispatch; 
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug(XmlUtility.toPrettyString(request.getSOAPPart()));
                }
                this.dispatch = this.getDispatch();
                try (Timer a = slice.getTimer("WebService.invoke." + this.name)) {
                    SOAPMessage response = dispatch.invoke(request);
                    return response;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error calling web service [" + this.name + "] with [" + XmlUtility.toPrettyString(request.getSOAPPart()) + "]", e);
            }
        }

        public void processResponse(SOAPMessage response, List<DirtyNode> dirtyNodes) {
            try {
                //body/_input/listofIc/Ic
                SOAPBody soapBody = response.getSOAPBody();
                SOAPElement outputElem = (SOAPElement) soapBody.getFirstChild();
                SOAPElement listOfElem = (SOAPElement) XmlUtility.getChildElement(outputElem, this.repositoryIntegrationObject.xmlTag);

                // Here we make sure we grab and process all icElements (used to process just the first one!)
                List<Element> icElements = XmlUtility.getChildElementsList(listOfElem);

                for (DirtyNode dirtyNode : dirtyNodes) {
                    boolean matchFound = false;
                    // Here instead of processing just the first icElement, we loop through them and try to match each of them with the current dirty node
                    for (int i = 0; i < icElements.size(); i++) {
                        SOAPElement icElement = (SOAPElement) icElements.get(i);
                        String icTagName = icElement.getTagName();
                        RepositoryIntegrationComponent ric = this.repositoryIntegrationObject.getRepositoryIntegrationComponentForTagName(icTagName);
                        if (!ric.getBusCompName().equals(dirtyNode.busCompName)) {
                            throw new IllegalArgumentException("Expecting icElement to correspond to dirty node");
                        }
                        List<RepositoryIntegrationComponentField> statusKeyFields = ric.getStatusKeyFields();
                        // If a match is found, then process the node and break out of the loop
                        if (doesElementMatchWithNode(icElement, dirtyNode, statusKeyFields)) {
                            logger.debug("Match found thus element at position " + i + " is removed!");
                            this.processResponse(icElement, dirtyNode);
                            icElements.remove(i);
                            matchFound = true;
                            break;
                        }
                    }

                    //If we are here, then a match is not found, thus, we just simply grab the first icElement and assume it matches with the current dirtyNode.
                    if (!matchFound) {
                        SOAPElement icElement = (SOAPElement) icElements.get(0);
                        logger.debug("Match not found thus element at position 0 is removed!");
                        this.processResponse(icElement, dirtyNode);
                        icElements.remove(0);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Cannot process reponse message for [" + XmlUtility.toPrettyString(response.getSOAPPart()) + "]", e);
            }
        }

        public void processResponse(SOAPElement icElement, DirtyNode dirtyNode) {
            // given the tag name for the ic, look up the ric
            String icTagName = icElement.getTagName();
            RepositoryIntegrationComponent ric = this.repositoryIntegrationObject.getRepositoryIntegrationComponentForTagName(icTagName);
            if (!ric.getBusCompName().equals(dirtyNode.busCompName)) {
                throw new IllegalArgumentException("Expecting icElement to correspond to dirty node");
            }
            List<RepositoryIntegrationComponentField> statusKeyFields = ric.getStatusKeyFields();
            for (RepositoryIntegrationComponentField ricf : statusKeyFields) {
                for (Element childElement : XmlUtility.getChildElementsList(icElement)) {
                    if (childElement.getTagName().equals(ricf.xmlTag)) {
                        String childValue = childElement.getTextContent();
                        String busCompFieldName = ricf.getBusCompFieldName();
                        DLFieldDefinition fieldDef = dirtyNode.entity.entityDefinition.getSiebelFieldDefinition(busCompFieldName);
                        if (fieldDef != null) {
                            logger.debug("Updating entity [" + dirtyNode.entity.getTagName() + "] set [" + fieldDef.getFieldName() + "] to [" + childValue + "]");
                            //dirtyNode.entity.addField(fieldDef.getFieldName(), childValue);
                            dirtyNode.entity.initialSetField(fieldDef.getFieldName(), childValue);
                            // this should be a trigger but i am hard coding it until we support field level triggers
                            if (dirtyNode.entity.getTagName().equals("HLSCase") && fieldDef.getFieldName().equals("MasterCaseNumber") && notEmpty(childValue)) {
                                String masterCaseNumber = childValue;
                                Slice entitySlice = dirtyNode.entity.getDataLayerSlice();
                                if (entitySlice != null) {
                                    DLEntity pubMasterCaseEntity = entitySlice.getPubMasterCaseEntity();
                                    if (pubMasterCaseEntity != null) {
                                        pubMasterCaseEntity.addField("MasterCaseNumber", masterCaseNumber);
                                        logger.debug("Updating entity [" + pubMasterCaseEntity.getTagName() + "] set [MasterCaseNumber] to [" + masterCaseNumber + "]");
                                    }
                                    DLEntity masterCaseEntity = entitySlice.getMasterCaseEntity();
                                    if (masterCaseEntity != null) {
                                        masterCaseEntity.addField("Number", masterCaseNumber);
                                        logger.debug("Updating entity [" + masterCaseEntity.getTagName() + "] set [Number] to [" + masterCaseNumber + "]");
                                    }
                                    DLNode dataLayerSliceNode = entitySlice.getRoot();
                                    if (dataLayerSliceNode != null) {
                                        DLEntity dataLayerSliceEntity = (DLEntity) dataLayerSliceNode;
                                        dataLayerSliceEntity.addField("MasterCaseNumber", masterCaseNumber);
                                        logger.debug("Updating entity [" + dataLayerSliceEntity.getTagName() + "] set [MasterCaseNumber] to [" + masterCaseNumber + "]");
                                    }
                                }
                            }
                        } // If field definition is null we try to blindly set the field using setField directly on the field name
                        else {
                            logger.debug("Blindly updating entity [" + dirtyNode.entity.getTagName() + "] set [" + StringUtility.toJavaName(busCompFieldName) + "] to [" + childValue + "]");
                            dirtyNode.entity.initialSetField(StringUtility.toJavaName(busCompFieldName), childValue);
                        }
                    }
                }
            }
            logger.debug("try recursion..");
            for (RepositoryIntegrationComponent childRic : this.repositoryIntegrationObject.getRepositoryIntegrationComponentWithParentName(ric.name)) {

                for (Element containerElement : XmlUtility.getChildElementsList(icElement)) {
                    if (containerElement.getTagName().equals(childRic.XmlContainerElement)) {
                        String childBusCompName = childRic.getBusCompName();

                        // go thru the child dirty nodes to find the one for the proper bc
                        List<Element> childElementList = XmlUtility.getChildElementsList(containerElement);

                        // get the group for the bc
                        List<DirtyNode> childBcDirtyNodeList = null;

                        for (List<DirtyNode> g : dirtyNode.groupedchildNodes) {
                            if (g.size() > 0 && g.get(0).busCompName.equals(childBusCompName)) {
                                childBcDirtyNodeList = g;
                                break;
                            }
                        }

                        if (childBcDirtyNodeList == null) {
                            throw new RuntimeException("Error processing response element [" + XmlUtility.toPrettyString(containerElement) + " response gave us  [" + childElementList.size() + "] elements but we are not expecting any!");
                        }

                        // here we assume 1:1 child dirty nodes to child elements
                        if (childBcDirtyNodeList.size() != childElementList.size()) {
                            throw new RuntimeException("Error processing response element [" + XmlUtility.toPrettyString(containerElement) + " expecting [" + childBcDirtyNodeList.size() + "] child elements");
                        }

                        // Here we loop through each child Element and attempt to find a matching dirty node
                        for (int i = 0; i < childElementList.size(); i++) {
                            SOAPElement childElement = (SOAPElement) childElementList.get(i);
                            String childICTagName = childElement.getTagName();
                            RepositoryIntegrationComponent childRIC = this.repositoryIntegrationObject.getRepositoryIntegrationComponentForTagName(childICTagName);
                            // This method will give us all status key fields and will contain the Id field in the beginning so we always match this first!
                            List<RepositoryIntegrationComponentField> childStatusKeyFields = childRIC.getStatusKeyFields();
                            boolean matchFound = false;

                            // Here we loop through each dirty node and try to match them
                            for (int j = 0; j < childBcDirtyNodeList.size(); j++) {
                                DirtyNode childDirtyNode = childBcDirtyNodeList.get(j);
                                if (!childRIC.getBusCompName().equals(childDirtyNode.busCompName)) {
                                    throw new IllegalArgumentException("Expecting icElement to correspond to dirty node");
                                }
                                // If a match is found, then process the node and break out of the loop
                                if (doesElementMatchWithNode(childElement, childDirtyNode, childStatusKeyFields)) {
                                    logger.debug("Match found thus element at position " + j + " is removed!");
                                    this.processResponse(childElement, childDirtyNode);
                                    childBcDirtyNodeList.remove(j);
                                    matchFound = true;
                                    break;
                                }
                            }
                            //If we are here, then a match is not found, thus we just simply grab the first element of the childBcDirtyNodeList and assume it matches 
                            //the current child element. We can't do any better!
                            if (!matchFound) {
                                logger.debug("Match not found thus element at position 0 is removed!");
                                this.processResponse(childElement, childBcDirtyNodeList.get(0));
                                childBcDirtyNodeList.remove(0);
                            }

                        }
                    }
                }
            }
        }

        private boolean doesElementMatchWithNode(SOAPElement childElement, DirtyNode childDirtyNode, List<RepositoryIntegrationComponentField> childStatusKeyFields) {
            // Loop through all status key fields
            boolean matched = false;
            for (RepositoryIntegrationComponentField childRICF : childStatusKeyFields) {
                // Loop through each field in the current child IC element and find the matching element with the status field
                for (Element ce : XmlUtility.getChildElementsList(childElement)) {
                    // Check if the tag names match and if they do then go try to match the node and element
                    if (ce.getTagName().equals(childRICF.xmlTag)) {
                        String childElementValue = ce.getTextContent();
                        String busCompFieldName = childRICF.getBusCompFieldName();
                        DLFieldDefinition fieldDef = childDirtyNode.entity.entityDefinition.getSiebelFieldDefinition(busCompFieldName);
                        if (fieldDef != null) {
                            String dirtyNodeValue = childDirtyNode.entity.getField(fieldDef.getFieldName());
                            // First we check if the Id's match. Id is guaranteed to be the first key!  
                            if (ce.getTagName().equals("Id")) {
                                logger.debug("ID childElementValue " + childElementValue);
                                logger.debug("ID dirtyNodeValue " + dirtyNodeValue);
                                if (dirtyNodeValue.equals(childElementValue)) {
                                    return true;
                                }
                            } else {
                                // Only get to this path if Id fields don't match, then we try to match other fields. 
                                logger.debug("childElementValue " + childElementValue);
                                logger.debug("dirtyNodeValue " + dirtyNodeValue);
                                if (!dirtyNodeValue.equals(childElementValue)) {
                                    return false;
                                }
                                matched = true;
                            }
                        }
                    }
                }
            }
            // This means everything but Id matched!
            return matched;
        }

        boolean allHaveSameBusCompName(List<DirtyNode> dirtyNodes) {
            if (dirtyNodes.size() == 0) {
                return true;
            }
            String busCompName = dirtyNodes.get(0).busCompName;
            for (DirtyNode dirtyNode : dirtyNodes) {
                if (!dirtyNode.busCompName.equals(busCompName)) {
                    return false;
                }
            }
            return true;
        }

        public void insertFields(DirtyNode dirtyNode, Set<String> addedFields, SOAPElement bcElement, RepositoryIntegrationComponent ric, boolean createUsingOldValues) throws SOAPException {
            String busCompName = dirtyNode.busCompName;
            SiebelBusCompEntity entity = dirtyNode.entity;
            for (DLField field : entity.getCachedDLFields()) {
                // BUG: seems like our entity is tied to the wrong entity definition. For example, this 
                // entity definition seems to be the self lookup one instead of the parent lookup one. 
                // as a result, we cannot see intertable fields.
                SiebelBusCompEntityDefinition entityDef = entity.entityDefinition;
                String busCompFieldName = entityDef.getFieldNativeName(field.getFieldName());
                //DLFieldDefinition fieldDef = entity.entityDefinition.getFieldDefinition(field.getFieldName());
                if (notEmpty(busCompFieldName)) {
                    //String busCompFieldName = fieldDef.getNativeName();
                    RepositoryIntegrationComponentField ricf = ric.getRepositoryIntegrationComponentFieldForBusCompField(busCompFieldName);
                    if (ricf != null) {
                        if (!addedFields.contains(ricf.xmlTag)) {
                            String fieldValue;
                            if (createUsingOldValues && field.hasAttribute("old_value")) {
                                fieldValue = field.getAttribute("old_value");
                                // If we are dealing with referenced fields, then we know that this value is not really "updated"
                                if (fieldValue.equals("OGSPECIALDLEMPTY")) {
                                    fieldValue = field.getFieldValue();
                                }
                            } else {
                                fieldValue = field.getFieldValue();
                            }

                            // do not insert nulls (only causes problems)
                            if (fieldValue != null && !fieldValue.isEmpty()) {
                                fieldValue = SiebelTypesUtility.toWebServiceFormat(ricf.externalDataType, fieldValue);
                                DLFieldDefinition fieldDef = entity.entityDefinition.getFieldDefinition(field.getFieldName());
                                // Check if normal fieldDefinition exists
                                if (fieldDef != null) {
                                } // Try to find workaround way
                                else {
                                    // Check if our current dirty node has a parent dirty node;
                                    if (entity.getParentNode() != null && entity.getParentNode().getParentNode() != null) {
                                        String linkName = entity.getParentNode().getTagName();
                                        if (dirtyNode.operations.contains(DirtyPath.Operation.Delete)) {
                                            linkName = linkName.substring(8);
                                        }
                                        String parentBusCompName = entity.getParentNode().getParentNode().getTagName();
                                        try {
                                            DLEntityDefinition childEntityDefinition = this.staticSlice.dataLayer.metaDataCache.getLinkedSiebelBusCompDefinition(parentBusCompName, linkName);
                                            fieldDef = childEntityDefinition.getFieldDefinition(field.getFieldName());
                                            if (fieldDef != null) {
                                            } else {
                                                logger.debug("Cannot find field defintion for field: " + field.getFieldName() + " for BC: " + busCompName);
                                            }

                                        } catch (Exception e) {
                                            throw new RuntimeException("Cannot find a child entity definition for the following Parent BC: " + parentBusCompName + " and Link: " + linkName);
                                        }
                                    }

                                }
                                SOAPElement fieldElement = bcElement.addChildElement(ricf.xmlTag, "ns2").addTextNode(fieldValue);
                                addedFields.add(fieldElement.getLocalName());
                            }
                        }
                    } else {
                        logger.warn("Cannot locate busCompField [" + busCompFieldName + "] in IC [" + ric.name + "]");
                    }
                } else {
                    logger.warn("Cannot find field definition for busCompField [" + field.getFieldName() + "] on BC [" + entity.getTagName() + "]");
                }
            }
        }

        public boolean[] insertUserKeyFields(DirtyNode dirtyNode, Set<String> addedFields, SOAPElement bcElement, RepositoryIntegrationComponent ric, AtomicInteger nextDummyId, boolean createUsingOldValues) throws SOAPException {
            boolean isIdInUserKey = false;
            boolean isUserKeyFieldsUnchanged = true;
            SiebelBusCompEntity entity = dirtyNode.entity;
            List<RepositoryIntegrationComponentField> userKeyRicfs = ric.getUserKeyRepositoryIntegrationComponentFields();
            for (RepositoryIntegrationComponentField ricf : userKeyRicfs) {
                //DLFieldDefinition fieldDef = this.getFieldDefinitionForRicField(entity, ricf);
                String siebelBCField = ricf.getBusCompFieldName();
                DLFieldDefinition fieldDef = entity.entityDefinition.getSiebelFieldDefinition(siebelBCField);
                if (fieldDef != null) {
                    DLField field = entity.getCachedDLField(fieldDef.getFieldName());
                    String fieldValue;
                    if (field != null) {
                        if (createUsingOldValues && field.hasAttribute("old_value")) {
                            fieldValue = field.getAttribute("old_value");
                            // If we are dealing with referenced fields, then we know that this value is not really "updated"
                            if (fieldValue.equals("OGSPECIALDLEMPTY")) {
                                fieldValue = field.getFieldValue();
                            }
                        } else {
                            fieldValue = field.getFieldValue();
                        }
                    } else {
                        fieldValue = entity.getField(fieldDef.getFieldName());
                    }
                    //String fieldValue = entity.getField(fieldDef.getFieldName());

                    isUserKeyFieldsUnchanged = isUserKeyFieldsUnchanged && !(field != null && field.hasAttribute("old_value") && !field.getAttribute("old_value").equals(field.getFieldValue()));
                    String busCompFieldName = fieldDef.getNativeName();
                    if (busCompFieldName.equals("Id")) {
                        isIdInUserKey = true;
                        if (fieldValue.equals("")) {
                            fieldValue = String.valueOf(nextDummyId.incrementAndGet());
                        }
                    }
                    fieldValue = SiebelTypesUtility.toWebServiceFormat(ricf.externalDataType, fieldValue);
                    SOAPElement fieldElement = bcElement.addChildElement(ricf.xmlTag, "ns2").addTextNode(fieldValue);
                    addedFields.add(fieldElement.getLocalName());
                } else {
                    throw new RuntimeException("Unexpected cannot find BusComp field to populate repository integration component field [" + ricf.name + "] on IC=[" + ric.name + "] to persist BusComp [" + entity.getNativeName() + "]");
                }
            }
            boolean[] output = {isIdInUserKey, isUserKeyFieldsUnchanged};
            return output;
        }

        public void populateMessage(SOAPMessage msg, List<DirtyNode> dirtyNodes) throws SOAPException {
            if (!allHaveSameBusCompName(dirtyNodes)) {
                throw new IllegalArgumentException();
            }
            String busCompName = dirtyNodes.get(0).busCompName;
            SiebelBusCompEntity entity = dirtyNodes.get(0).entity;

            boolean setMVG = false;
//            boolean setMVG = dirtyNodes.get(0).operations.contains(DirtyPath.Operation.SetMVG) && 
//                    entity.getLink("OGDL_PrimaryParentEntities") != null &&
//                    entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().size() > 0;

            if (!busCompName.equals(this.repositoryIntegrationObject.getRootBusCompName())) {
                throw new IllegalArgumentException();
            }
            SOAPElement inputElement = (SOAPElement) msg.getSOAPBody().getFirstChild();
            RepositoryIntegrationComponent ric = this.repositoryIntegrationObject.getRootRepositoryIntegrationComponent(setMVG);
            //logger.debug("INPUT ELEM:" + XmlUtility.toPrettyString(inputElement));
            SOAPElement listOfElement = inputElement.addChildElement(ric.getListOfTagName(), "ns2");
            AtomicInteger nextDummyId = new AtomicInteger(0);
            for (DirtyNode dirtyNode : dirtyNodes) {
                populateMessage("", listOfElement, dirtyNode, nextDummyId, null, false);
            }
        }
//        

        public void populateMessage(String parentIntegrationComponentName, SOAPElement listOfElement, DirtyNode dirtyNode, AtomicInteger nextDummyId, SiebelBusCompEntity parEntity, boolean setMVG) throws SOAPException {
            String busCompName = dirtyNode.busCompName;
            SiebelBusCompEntity entity = dirtyNode.entity;
            if (entity.tagName.equals("CUTAddress")) {
                logger.debug("");
            }

            boolean isCurrentEntityMVG = dirtyNode.operations.contains(DirtyPath.Operation.SetMVG)
                    && dirtyNode.entity.getLink("OGDL_PrimaryParentEntities") != null
                    && dirtyNode.entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().size() > 0
                    && parEntity != null
                    && dirtyNode.entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().contains(parEntity);

            RepositoryIntegrationComponent ric = this.repositoryIntegrationObject.getRepositoryIntegrationComponentForBusComp(parentIntegrationComponentName, busCompName, setMVG);
            if (ric == null) {
                throw new RuntimeException("Cannot find a child integration components for busCompName [" + busCompName + "] with parent integration component name [" + parentIntegrationComponentName + "]");
            }
            SOAPElement bcElement = listOfElement.addChildElement(ric.xmlTag, "ns2");
            if (dirtyNode.operationAttributeValue != null && !dirtyNode.operations.contains(DirtyPath.Operation.Delete)) {
                bcElement.setAttribute("Operation", dirtyNode.operationAttributeValue);
            }

            if (dirtyNode.operations.contains(DirtyPath.Operation.Delete)) {
                bcElement.setAttribute("Operation", "delete");
            }
            if (isCurrentEntityMVG) {
                bcElement.setAttribute("IsPrimaryMVG", "Y");
            }

            // remember the fields tags we have added
            Set<String> addedFields = Sets.newHashSet();
            boolean[] decisionPoints;
            try {
                decisionPoints = insertUserKeyFields(dirtyNode, addedFields, bcElement, ric, nextDummyId, false);
            } catch (SOAPException e) {
                throw new RuntimeException(e);
            }

            boolean isIdInUserKey = decisionPoints[0];
            boolean isUserKeyFieldsUnchanged = decisionPoints[1];

            // If the operation is insert then add rest of the fields. If we are associating an entity that doesn not have an Id field is not in the user key 
            // then add the rest of the fields as well. This is to handle the situation where CUT Address user key is not the Id field. In this situation, 
            // we want to pass all the fields that CUT Address have like Type.
            // However, when we are updating an entity that doesn't have an Id in it's userkey then we check if any of the user key fields are changed
            // If it is changed then we then delete the entity then insert a new one. If the user key fields are not changed, then we simply populate the fields of the 
            // entity.
            if ((!isIdInUserKey && !entity.tagName.equals("OneGateEAIBenefitPlanLineItemReasonCode")) || dirtyNode.operations.contains(Operation.Insert)) {
                // This is the path to take if user key has changed and operation is update
                if (!isUserKeyFieldsUnchanged && dirtyNode.operations.contains(Operation.Update)) {
                    // Set the old fields and delete the old entity
                    try {
                        //Clear all fields out so we rebuild our deleted node from old values !
                        addedFields.clear();
                        bcElement.removeContents();
                        insertUserKeyFields(dirtyNode, addedFields, bcElement, ric, nextDummyId, true);
                        insertFields(dirtyNode, addedFields, bcElement, ric, true);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    bcElement.setAttribute("Operation", "delete");

                    // Then create the new entity
                    bcElement = listOfElement.addChildElement(ric.xmlTag, "ns2");
                    //bcElement.setAttribute("Operation", "insert");
                    addedFields = Sets.newHashSet();

                    try {
                        insertUserKeyFields(dirtyNode, addedFields, bcElement, ric, nextDummyId, false);
                    } catch (SOAPException e) {
                        throw new RuntimeException(e);
                    }
                    insertFields(dirtyNode, addedFields, bcElement, ric, false);

                    // Now we have to make sure our dirtyNodes match!! So we first create a new delete dirtyNode.
                    DirtyNode parentDirtyNode = dirtyNode.getParentDirtyNode();
                    if (parentDirtyNode != null) {
                        //This is an entity that is not hooked up to the Siebel tree. It's just hooked to our new cloned DirtyNode
                        SiebelBusCompEntity deletedEntity = createSiebelBCWithOldFields(entity);
                        DirtyNode clonedDeleteDirtyNode = new DirtyNode(deletedEntity, dirtyNode.dirtyPaths);
                        for (Operation operation : clonedDeleteDirtyNode.operations) {
                            if (operation.equals(Operation.Update)) {
                                operation = Operation.Delete;
                                break;
                            }
                        }
                        dirtyNode.parentDirtyNodeGroup.add(clonedDeleteDirtyNode);
                    }

                    // Next we update the operation of our update dirtyNode to be an Insert
                    for (Operation operation : dirtyNode.operations) {
                        if (operation.equals(Operation.Update)) {
                            operation = Operation.Insert;
                            break;
                        }
                    }
                } // Normal Insert Path or you are associating a non Id based user key entity or if we are updating an non Id based user key entity whose user keys haven't changed
                else {
                    try {
                        insertFields(dirtyNode, addedFields, bcElement, ric, false);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                // TBD: check that we have values for all the required fields!
                // TBD: or check see if we can get away w/ passing null for them. 
            } // else if update add the changed fields
            else if (dirtyNode.operations.contains(Operation.Update)) {

                Map<String, String> updatedValuesPersistLocations = new HashMap();
                for (DLField field : entity.getCachedDLFields()) {
                    DLFieldDefinition fieldDef = entity.entityDefinition.getFieldDefinition(field.getFieldName());
                    // forces updates of hls case to write contact person id field
                    if (field.hasAttribute("old_value") || (entity.getTagName().equals("HLSCase") && field.getTagName().equals("ContactPersonId"))) {
                        if (fieldDef != null) {
                            String busCompFieldName = fieldDef.getNativeName();
                            RepositoryIntegrationComponentField ricf = ric.getRepositoryIntegrationComponentFieldForBusCompField(busCompFieldName);
                            if (ricf != null) {
                                if (!addedFields.contains(ricf.xmlTag)) {
                                    String tableAlias = fieldDef.getTableAlias();
                                    String columnName = fieldDef.getColumnName();
                                    String persistLocation = tableAlias + "***" + columnName;
                                    String fieldValue = field.getFieldValue();
                                    fieldValue = SiebelTypesUtility.toWebServiceFormat(ricf.externalDataType, fieldValue);
                                    //Now check our map to see if another field that persists in the same DB table and column
                                    //has been marked for persistence already! 
                                    if (updatedValuesPersistLocations.containsKey(persistLocation)) {
                                        //If it exists,check if the fieldValue that we are adding is the same as the one that we have added before.
                                        //If not we throw an exception because we are trying to set a field to different values!!
                                        String oldFieldValue = updatedValuesPersistLocations.get(persistLocation);
                                        if (!oldFieldValue.equals(fieldValue)) {
                                            throw new RuntimeException("Attempting to pass two different values to table [" + tableAlias + "] and column ["
                                                    + columnName + "]. Old Value: [" + oldFieldValue + "] New Value: [" + fieldValue + "]. Field name is ["
                                                    + busCompFieldName + "].");

                                        }
                                    } else {
                                        //If our field hasn't been marked for persistence yet then add it to the payload.
                                        updatedValuesPersistLocations.put(persistLocation, fieldValue);
                                        SOAPElement fieldElement = bcElement.addChildElement(ricf.xmlTag, "ns2").addTextNode(fieldValue);
                                        addedFields.add(fieldElement.getLocalName());
                                    }
                                }
                            } else {
                                logger.warn("Cannot locate busCompField [" + busCompFieldName + "] in IC [" + ric.name + "]");
                            }
                        }
                    }
                }

            }
            // do the child nodes only if we are not setting the current BC as the MVG
            if (!setMVG) {
                for (List<DirtyNode> childDirtyNodeGroup : dirtyNode.groupedchildNodes) {
                    String groupBusCompName = childDirtyNodeGroup.get(0).busCompName;

                    boolean setChildMVG = false;
                    for (DirtyNode childDirtyNode : childDirtyNodeGroup) {
                        if (childDirtyNode.operations.contains(DirtyPath.Operation.SetMVG)
                                && childDirtyNode.entity.getLink("OGDL_PrimaryParentEntities") != null
                                && childDirtyNode.entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().size() > 0
                                && childDirtyNode.entity.getLink("OGDL_PrimaryParentEntities").getChildEntities().contains(entity)) {
                            setChildMVG = true;
                            break;
                        }
                    }

                    RepositoryIntegrationComponent childRic = ric.getChildRepositoryIntegrationComponentForBusComp(groupBusCompName, setChildMVG);
                    logger.debug("using childRIc=[" + childRic.name + "] for [" + groupBusCompName + "]");
                    String childListOfElementName = childRic.XmlContainerElement;
                    SOAPElement childListOfElement = bcElement.addChildElement(childListOfElementName, "ns2");

                    List<DirtyNode> clonedChildDirtyNodeGroup = (ArrayList<DirtyNode>) ((ArrayList<DirtyNode>) childDirtyNodeGroup).clone();

                    for (DirtyNode childDirtyNode : clonedChildDirtyNodeGroup) {
                        this.populateMessage(childRic.parentIntegrationComponent, childListOfElement, childDirtyNode, nextDummyId, entity, setChildMVG);
                    }
                }
            }
        }

        private SiebelBusCompEntity createSiebelBCWithOldFields(SiebelBusCompEntity entity) {
            SiebelBusCompEntity newEntity = new SiebelBusCompEntity(entity.entityDefinition);
            for (DLField field : entity.getCachedDLFields()) {
                String fieldValue;
                if (field.hasAttribute("old_value")) {
                    fieldValue = field.getAttribute("old_value");
                    // If we are dealing with referenced fields, then we know that this value is not really "updated"
                    if (fieldValue.equals("OGSPECIALDLEMPTY")) {
                        fieldValue = field.getFieldValue();
                    }
                } else {
                    fieldValue = field.getFieldValue();
                }
                newEntity.setField(field.getFieldName(), fieldValue);
            }
            return newEntity;
        }

        public SOAPMessage buildMessage(List<DirtyNode> dirtyNodes) {
            try {
                MessageFactory mf;
                mf = MessageFactory.newInstance(SOAPConstants.SOAP_1_1_PROTOCOL);
                SOAPMessage MyMsg = mf.createMessage();
                SOAPPart MyPart = MyMsg.getSOAPPart();
                SOAPEnvelope MyEnv = MyPart.getEnvelope();
                SOAPHeader header = MyEnv.getHeader();
                Name headerQName = MyEnv.createName("Security", "wss", "http://schemas.xmlsoap.org/ws/2002/12/secext");
                SOAPHeaderElement headerElement = header.addHeaderElement(headerQName);
                SOAPElement usernameTokenElement = headerElement.addChildElement("UsernameToken", "wss");
                usernameTokenElement.addChildElement("Username", "wss").addTextNode("SADMIN");
                usernameTokenElement.addChildElement("Password", "wss").addTextNode("SADMIN123");
                SOAPBody MyBody = MyEnv.getBody();
                String inputTagName = this.name.replace(" ", "_spc") + "_Input";
                Name inputName = MyEnv.createName(inputTagName, null, this.namespace);
                SOAPBodyElement inputElement = MyBody.addBodyElement(inputName);
                String ns2 = "http://www.siebel.com/xml/" + this.repositoryIntegrationObject.name.replace(" ", "%20");
                inputElement.addNamespaceDeclaration("ns2", ns2);
                inputElement.addChildElement("StatusObject").addTextNode("true");
                this.populateMessage(MyMsg, dirtyNodes);
                logger.debug(XmlUtility.toPrettyString(MyPart));
                return MyMsg;
            } catch (Exception e) {
                throw new RuntimeException("Cannot create web service message for [" + this.name + "]", e);
            }
        }

        public boolean canProcessPath(DirtyPath dirtyPath) {
            int rootIdx = dirtyPath.bcNames.indexOf(this.repositoryIntegrationObject.getRootBusCompName());
            // if the web service root BC is the is not in the dirty path, then
            // this web service cannot process the path
            if (rootIdx < 0) {
                return false;
            }
            // if the operation is associate or delete and the webservice root BC is the dirtyBC then the 
            // web service cannot persist the link.
            // EDIT = removed delete
            if ((dirtyPath.operation.equals(DirtyPath.Operation.Associate)
                    //|| dirtyPath.operation.equals(DirtyPath.Operation.Delete)
                    || dirtyPath.operation.equals(DirtyPath.Operation.SetMVG))
                    && dirtyPath.getDirtySiebelBusCompName().equals(this.repositoryIntegrationObject.getRootBusCompName())) {
                return false;
            }

            RepositoryIntegrationComponent ric = this.repositoryIntegrationObject.getRootRepositoryIntegrationComponent(false);
            for (int dirtyIdx = rootIdx + 1; dirtyIdx <= dirtyPath.getMaxIndex(); dirtyIdx++) {
                String busCompName = dirtyPath.bcNames.get(dirtyIdx);
                boolean getMVG = dirtyPath.operation.equals(DirtyPath.Operation.SetMVG) && dirtyPath.getDirtySiebelBusCompName().equals(busCompName);
                RepositoryIntegrationComponent childRic = ric.getChildRepositoryIntegrationComponentForBusComp(busCompName, getMVG);
                if (childRic == null) {
                    return false;
                }
                ric = childRic;
            }
            return true;
        }
    }

    public class Heap {

        TreeSet tree = new TreeSet();

        public boolean add(Object o) {
            return tree.add(o);
        }

        public boolean pop() {
            return tree.remove(tree.first());
        }

        public Object top() {
            return tree.first();
        }

        public boolean isEmpty() {
            return tree.size() == 0;
        }
    }

    public class WebServiceCallStrategy implements Comparable {

        List<DirtyPath> remainingdirtyPaths;
        List<Map.Entry<WebService, List<DirtyPath>>> wsCalls;
        List<WebService> strategy = new ArrayList<>();

        public WebServiceCallStrategy(List<DirtyPath> remainingdirtyPaths, List<Map.Entry<WebService, List<DirtyPath>>> wsCallStrategy) {
            this.remainingdirtyPaths = remainingdirtyPaths;
            this.wsCalls = wsCallStrategy;
            if (!wsCallStrategy.isEmpty()) {
                for (Map.Entry<WebService, List<DirtyPath>> m : wsCallStrategy) {
                    strategy.add(m.getKey());
                }
            }
        }

        @Override
        public int compareTo(Object o) {
            WebServiceCallStrategy n = (WebServiceCallStrategy) o;
            // The better strategy should be the one which has less web service calls
            if (wsCalls.size() < n.wsCalls.size()) {
                return -1;
            }
            if (wsCalls.size() > n.wsCalls.size()) {
                return 1;
            }
            // If the number of web service calls are equal, the better strategy should be the one which 
            // has less remaining dirty paths
            if (remainingdirtyPaths.size() < n.remainingdirtyPaths.size()) {
                return -1;
            }
            if (remainingdirtyPaths.size() > n.remainingdirtyPaths.size()) {
                return 1;
            }
            // If all things are equal, the better strategy should be the one with less wasted calls
            int myWastedCalls = 0;
            int otherWastedCalls = 0;
            for (int i = 0; i < wsCalls.size(); i++) {
                myWastedCalls = closestToServiceRoot(wsCalls.get(i)) + myWastedCalls;
                otherWastedCalls = closestToServiceRoot(n.wsCalls.get(i)) + otherWastedCalls;
            }
            if (myWastedCalls < otherWastedCalls) {
                return -1;
            }
            if (myWastedCalls > otherWastedCalls) {
                return 1;
            }
            // Lastly, if 2 things are equal, then we just concatenate the webservice names each call uses and 
            // choose whichever one comes alphabetically in front of each other
            String myCurrentWebServiceStrategy = "";
            String otherWebServiceStrategy = "";
            for (int i = 0; i < wsCalls.size(); i++) {
                myCurrentWebServiceStrategy = strategy.get(i).name + myCurrentWebServiceStrategy;
                otherWebServiceStrategy = n.strategy.get(i).name + otherWebServiceStrategy;
            }
            return myCurrentWebServiceStrategy.compareTo(otherWebServiceStrategy);
        }
    }
}
