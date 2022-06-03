# compose_flask/app.py
from datetime import date, timedelta
import datetime as dt
import json
import time
import pandas as pd

### REQUIRED FOR NEO4J CONTAINER TO FINISH SETTING UP
time.sleep(10)

# ### Community Package
# from py2neo import Graph, Node, Relationship
# graph = Graph("http://neo4j:7474/db/data/")
# # begin a transaction
# tx = graph.begin()
# alice = Node("Person", name="Alice")
# tx.create(alice)
# tx.commit()

### Official Package
from neo4j import GraphDatabase

# with driver.session() as session:
#       #DROP CONSTRAINT ON (<label_name>) ASSERT <property_name> IS UNIQUE
#       session.run("CREATE CONSTRAINT ON (d:datetime) ASSERT d.name IS UNIQUE")
#       session.run("CREATE CONSTRAINT ON (d:datetime) ASSERT d.time IS UNIQUE")

'''
GRAPH FUNCTIONS
Contains the functions to modify the graph.
'''
class _GraphFunctions:
      def __init__(self):
            uri = "bolt://0.0.0.0:7687/db/data"
            self.driver = None
            try:
                  self.driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))
            except Exception as e:
                  print("Failed to create the driver:", e)
          
### ================================= GRAPH QUERY FUNCTIONS ===================================== ###

### ================================= GRAPH MANAGEMENT FUNCTIONS ===================================== ###
      '''
      CREATE NODE
      parameters: 
      - node_label(list) = List of Node types (Location, Person, Organisation), 
      - node_attributes(dict) = Attributes of Node (Name, Address, Location ,Coordinates) 
      '''
      # create_node creates duplicates and causes memory problems, use merge_node instead
      def merge_node(self, node_labels, node_attributes):
            with self.driver.session() as session:
                  node_labels = ":".join(node_labels)
                  node_attributes = "{"+", ".join([k+" : '"+str(node_attributes[k])+"'" for k in node_attributes.keys()])+"}"
                  #print("MERGE (p:{} {}) RETURN p".format(node_label, node_attributes))'
                  return session.run("MERGE (p:{} {}) RETURN p".format(node_labels, node_attributes)).single().value() 

      '''
      CREATE EDGE
      parameters: 
      - source_node (redis node object) = starting node of edge
      - target_node (redis node object) = ending node of edge
      - relationship_label (str) = relationship between the nodes (contact location, role in organisation)
      - edge_attributes(dict) = Attributes of relationship (Name, Address, Location ,Coordinates) 
      '''
      # create_edge creates duplicates and causes memory problems, use merge_edge instead

      def merge_edge(self, source_node_label, source_node_attribute, target_node_label, target_node_attribute, relation_type, edge_attributes):
            #DIRECTED
            #"MATCH (s:{} {{name: '{}'}}), (t:{} {{name:'{}'}}) CREATE (s)-[e:{} {}]->(t) RETURN e".format(source_node_label, source_node_name, target_node_label, target_node_name, relation_type ,edge_attributes)
            #UNDIRECTED
            with self.driver.session() as session:
                  source_attributes = "{"+", ".join([k+" : '"+str(source_node_attribute[k])+"'" for k in source_node_attribute.keys()])+"}"
                  target_attributes = "{"+", ".join([k+" : '"+str(target_node_attribute[k])+"'" for k in target_node_attribute.keys()])+"}"
                  edge_attributes = "{"+", ".join([k+" : '"+edge_attributes[k]+"'" for k in edge_attributes.keys()])+"}"
                  return session.run("MATCH (s:{} {}), (t:{} {}) MERGE (s)-[e:{} {}]-(t) RETURN e".format(source_node_label, source_attributes, target_node_label, target_attributes, relation_type ,edge_attributes))#.single().value()

      '''
      CREATE INDEX
      A named b-tree index on a single property for all nodes with a particular label.
      parameters: 
      - node_index_name(str) = Name of index (must be unique)
      - node_label(str) = Node type to be indexed (Location, Person, Organisation), 
      - node_attribute(str) = Attributes of Node to be indexed (Name, Address, Location ,Coordinates) 
      '''
      def create_index(self, node_index_name, node_label, node_attribute):
            with self.driver.session() as session:
                  return session.run("CREATE INDEX {} IF NOT EXISTS FOR (n:{}) ON (n.{})".format(node_index_name, node_label, node_attribute))#.single().value()

      '''
      USING GENERAL COMMANDS -   
      DELETE is used to remove both nodes and relationships. Note that deleting a node 
      also deletes all of its incoming and outgoing relationships.
      To delete a node and all of its relationships:

      DELETE NODE
      parameters: 
      - node_label(str) = Node type to delete 
      - attribute_del_type(str) = Attribute key/type used as filter condition to delete(match by name/location/id)
      - attribute_value(str) = filter condition value 

      EXAMPLE:
      GRAPH.QUERY DEMO_GRAPH "MATCH (p:person {name:'Jim'}) DELETE p"
      '''
      def delete_node(self, node_label, attribute_del_type, attribute_value):
            ### ADD PARAMETERS ASSERTION HERE ###
            with self.driver.session() as session:
                  return session.run("MATCH (n:{} {'{}':'{}'}) DELETE n".format(node_label, attribute_del_type, attribute_value)).single().value()

      '''
      DELETE EDGE
      This query will delete all 'relationship_label' outgoing relationships from the node with the attribute_del_type = attribute_value.
      parameters: 
      - node_label(str) = Node type to delete 
      - attribute_del_type(str) = Attribute key/type used as filter condition to delete(match by name/location/id)
      - attribute_value(str) = filter condition value

      EXAMPLE:
      GRAPH.QUERY DEMO_GRAPH "MATCH (:person {name:'Jim'})-[r:friends]->() DELETE r"
      '''
      def delete_edge(self, node_label, attribute_del_type, attribute_value, relationship_label):
            ### ADD PARAMETERS ASSERTION HERE ###
            with self.driver.session() as session:
                  return session.run("MATCH (:{} {'{}':'{}'})-[r:{}]->() DELETE r".format(node_label, attribute_del_type, attribute_value, relationship_label)).single().value()

      '''
      EDIT NODE
      USING GENERAL REDIS COMMANDS - 
      SET is used to create or update properties on nodes and relationships. Must use a unique node identifier as match condition

      EXAMPLE:
      GRAPH.QUERY DEMO_GRAPH "MATCH (n { name: 'Jim' }) SET n.name = 'Bob'"
      '''
      def edit_node_attribute(self, attribute_edit_type, attribute_value, new_attribute_value):
            ### ADD PARAMETERS ASSERTION HERE ###
            with self.driver.session() as session:
                  return session.run("MATCH (n: { '{}': '{}' }) SET n.{} = '{}'".format(attribute_edit_type, attribute_value, attribute_edit_type, new_attribute_value)).single().value()


      def clear_graph(self):
            with self.driver.session() as session:
                  session.run("MATCH p=()-->() DELETE p")
                  return None

'''
GRAPH GENERATOR
This class will process the dataframes and populate NEO4J with the data from the dataframes.
'''
class GraphGenerator:
      def __init__(self, node_df, relation_df, triple_df):
            self.node_df = node_df
            self.relation_df = relation_df.set_index("relation_id")
            self.triple_df = triple_df
            self.graph = _GraphFunctions()
      
      '''
      GENERATE GRAPH FROM DATAFRAME
      This function will populate NEO4J with the data from the dataframes.
      '''
      def generate_graph_from_df(self):
            self._generate_nodes()
            self._generate_edges()

      def _generate_nodes(self):
            for idx, node in self.node_df.iterrows():
                  # Every node has a universal dummy label "Node"
                  node_labels = [node.entity_type, "Node"]
                  node_attributes = {"name": node.node_name, "node_id": node.node_id}
                  self.graph.merge_node(node_labels, node_attributes)
            
            self.graph.create_index("node_id_index", "Node", "node_id")
            
      def _generate_edges(self):
            for idx, triple in self.triple_df.iterrows():
                  # Can just use the universal node label since node_id already uniquely identifies the node
                  source_node_label = "Node"
                  source_node_attributes = {"node_id": triple.subject}
                  target_node_label = "Node"
                  target_node_attributes = {"node_id": triple.object}
                  relation_type = self.relation_df["relation"][triple.relation]
                  edge_attributes = {"relation_id": triple.relation}
                  self.graph.merge_edge(source_node_label, source_node_attributes, target_node_label, target_node_attributes, relation_type, edge_attributes)

if __name__ == "__main__":
      # Dummy data for testing
      nodes = [["Obama", 1, "PPL"], ["USA", 2, "CTY"]]
      relations = [["Citizen", "RS1"]]
      triples = [[1, "RS1", 2]]
 
      node_df = pd.DataFrame(nodes, columns=['node_name', 'node_id', 'entity_type'])
      relation_df = pd.DataFrame(relations, columns=['relation', 'relation_id'])
      triple_df = pd.DataFrame(triples, columns=['subject', 'relation', 'object'])

      gen = GraphGenerator(node_df, relation_df, triple_df)
      gen.generate_graph_from_df()
