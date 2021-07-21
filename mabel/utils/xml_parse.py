# https://stackoverflow.com/questions/2148119/how-to-convert-an-xml-string-to-a-dictionary

from xml.etree import cElementTree as ElementTree

class XmlListConfig(list):
    def __init__(self, aList):
        for element in aList:
            if element:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):

    def __init__(self, parent_element):
        self[parent_element.tag] = self.get(parent_element.tag, {})
        if parent_element.items():
            for k, v in parent_element.items():
                self[parent_element.tag][f"@{k}"] = v
        for element in parent_element:
            if element:
                self[parent_element.tag][element.tag] = self[parent_element.tag].get(element.tag, {})
                # treat like dict if all of the element tags are the different
                # we are making the assumption that the children are either all
                # different, or all the same, not mixed  
                if len(element) == len({element[i].tag for i in range(len(element))}):
                    self[parent_element.tag].update(XmlDictConfig(element))
                else:
                    self[parent_element.tag][element.tag][element[0].tag] = XmlListConfig(element)

                # if the tag has attributes, add those to the dict
                if element.items():
                    for k, v in element.items():
                        self[parent_element.tag][element.tag][f"@{k}"] = v
                print('over', element.tag)


            self[parent_element.tag][element.tag] = self[parent_element.tag].get(element.tag, {})
            for k, v in element.items():
                self[parent_element.tag][element.tag][f"@{k}"] = v

            if element.text and element.text.strip():
                if self[parent_element.tag][element.tag] != {}:
                    self[parent_element.tag][element.tag]["#text"] = element.text.strip()
                else:
                    self[parent_element.tag][element.tag] = element.text.strip()


def strip_namespace(entry):
    if isinstance(entry, dict):
        for k in [k for k in entry.keys() if k.startswith("{")]:
            k2 = k.split('}', 1)[1]
            entry[k2] = entry.pop(k)
        for k in [k for k in entry.keys() if k.startswith("@{")]:
            k2 = k.split('}', 1)[1]
            entry[f"@{k2}"] = entry.pop(k)
        for child in entry:
            if isinstance(entry[child], (list, dict)):
                strip_namespace(entry[child])
    if isinstance(entry, list):
        for child in entry:
            if isinstance(child, (list, dict)):
                strip_namespace(child)

def parse(xml_string):
    tree = ElementTree.XML(xml_string)
    dictionary = XmlDictConfig(tree)
    strip_namespace(dictionary)
    return dictionary

