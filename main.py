from owlready2 import *
from neo4j import GraphDatabase


class GraphDb:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    data_type_dict = {
        "float": "decimal",
        "int": "integer",
        "str": "string",
        "bool": "boolean",
        "datetime.datetime": "decimal",
        "owlready2.util.normstr": "string",

    }

    def close(self):
        self.driver.close()

    def print_node(self, message, node_type):
        with self.driver.session() as session:
            session.write_transaction(self._create_node, message, node_type)

    def print_subclass_is_a_relationship(self, node_from, node_to):
        with self.driver.session() as session:
            session.write_transaction(self._create_subclass_relationship, node_from, node_to)

    def print_individual_is_a_relationship(self, individual_, node_):
        with self.driver.session() as session:
            session.write_transaction(self._create_individual_is_a_relationship, individual_, node_)

    def print_object_property_relationship(self, individual_1, individual_2, object_property_):
        with self.driver.session() as session:
            session.write_transaction(self._create_object_property_relationship,
                                      individual_1,
                                      individual_2,
                                      object_property_)

    @staticmethod
    def _create_node(tx, message, node_type):
        if node_type == "node_class":
            tx.run("CREATE (n: Class) "
                   "SET n.name = '" + message + "' RETURN n.name")
        elif node_type == "node_individual":
            tx.run("CREATE (n: Individual) "
                   "SET n.name = '" + message + "' RETURN n.name")

    @staticmethod
    def _create_subclass_relationship(tx, node_from, node_to):
        tx.run("MATCH(a: Class), (b: Class) "
               "WHERE a.name = $a AND b.name = $b "
               "CREATE (a) - [r: is_a]->(b) RETURN r", a=node_from, b=node_to)

    @staticmethod
    def _create_individual_is_a_relationship(tx, individual_, node_):
        tx.run("MATCH(a: Individual), (b: Class) "
               "WHERE a.name = $a AND b.name = $b "
               "CREATE (a) - [r: is_a]->(b) RETURN r", a=individual_, b=node_)

    @staticmethod
    def _create_object_property_relationship(tx, individual_1, individual_2, object_property_):
        tx.run("MATCH(a: Individual), (b: Individual) "
               "WHERE a.name = '" + individual_1 + "' AND b.name = '" + individual_2 + "' "
               "CREATE (a) - [r: " + object_property_ + "]->(b) RETURN r")

    @staticmethod
    def _create_data_type(tx, d_type_, value_, individual_, object_property_):
        query = "MATCH (a:Individual) WHERE a.name = '" \
                + individual_ + "' CREATE (a) -[r: " + object_property_ + "]->(b:DataType {value: '" \
                + value_ + "', data_type: '" + d_type_ + "'})"
        tx.run(query)

    def print_data_property(self, individual_, object_property_, d_type_, value_):
        with self.driver.session() as session:
            session.write_transaction(self._create_data_type, d_type_, value_, individual_, object_property_)


if __name__ == "__main__":
    onto = get_ontology("file://ariac-rdf-xml.owl").load()
    gdb = GraphDb("bolt://localhost:7687", "neo4j", "ariac")
    # Create the Thing node
    gdb.print_node("Thing", "node_class")
    gdb.print_node("DataThing", "node_class")
    gdb.print_node("SolidObject", "node_class")
    gdb.print_subclass_is_a_relationship("DataThing", "Thing")
    gdb.print_subclass_is_a_relationship("SolidObject", "Thing")
    is_db = True
    for class_ in onto.classes():
        if class_.name in "DataThing":
            pass
        elif class_.name in "SolidObject":
            pass
        else:
            if is_db:
                gdb.print_node(class_.name, "node_class")
            else:
                pass

    for class_ in onto.classes():
        # print(class_, list(class_.subclasses()))
        # Get all subclasses
        for sub_class_ in list(class_.subclasses()):
            if is_db:
                gdb.print_subclass_is_a_relationship(sub_class_.name, class_.name)

        for instance_ in onto.get_instances_of(class_):
            if is_db:
                gdb.print_node(instance_.name, "node_individual")
                gdb.print_individual_is_a_relationship(instance_.name, class_.name)
            else:
                pass
            # print(f"{instance_.name} is-a {class_.name}")
    #         original_i = i.name
    #         for prop in i.get_properties():
    #             # i.name = original_i
    #             print(f"    ---{i} : {prop}")
    #             # print(f"    ---{prop} -- {prop.python_name}")
    #             # prop.python_name = "property_"
    #             # i.name = "i_"
    #             # i_name = i.name
    #             # print(f"    ---{i_name} : {prop}")
    #             # print(i.name, i_prop.name, i_prop.domain)
    #             # prop_name = (type(prop))(prop.python_name)
    #             # print(f"---{prop_name} -- {type(prop_name)}")
    #             #
    #             # print(eval(i.name))
    #             # print(f"        +++++ {onto.i_name.property_}")
    #             print(f"        +++++ {list(prop.get_relations())}")

    for object_property in onto.object_properties():
        for relations in list(object_property.get_relations()):
            if is_db:
                gdb.print_object_property_relationship(relations[0].name,
                                                       relations[1].name,
                                                       object_property.python_name)

    for d_p in onto.data_properties():
        data_type = str(d_p.range)[9:]
        size = len(data_type)
        data_type = data_type[:size - 3]

        for d_relation in list(d_p.get_relations()):
            if is_db:
                gdb.print_data_property(d_relation[0].name, d_p.python_name, gdb.data_type_dict[data_type],
                                        str(d_relation[1]))

    gdb.close()
