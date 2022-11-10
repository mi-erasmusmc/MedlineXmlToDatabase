/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package ohdsi.utilities;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.List;

public class XmlTools {
    public static boolean isTextNode(Node node) {
        NodeList children = node.getChildNodes();
        for (int j = 0; j < children.getLength(); j++) {
            String name = children.item(j).getNodeName();
            if (("#text".equals(name) && children.item(j).getNodeValue() != null && children.item(j).getNodeValue().trim().length() != 0) || "b".equals(name)
                    || name.equals("i") || name.equals("sup") || name.equals("sub"))
                return true;
        }
        return false;
    }

    public static boolean isTextNode(org.dom4j.Node node) {
        List<org.dom4j.Node> children = node.selectNodes("./*");
        for (org.dom4j.Node child : children) {
            String name = child.getName();
            if (("#text".equals(name) && child.getStringValue() != null && child.getStringValue().trim().length() != 0) || "b".equals(name)
                    || name.equals("i") || name.equals("sup") || name.equals("sub"))
                return true;
        }
        return false;
    }

    public static Node getChildByName(Node node, String name) {
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node childNode = childNodes.item(i);
            if (childNode.getNodeName().equals(name))
                return childNode;
        }
        return null;
    }

    public static String getChildByNameValue(Node node, String name) {
        Node childNode = getChildByName(node, name);
        if (childNode == null)
            return null;
        else
            return getValue(childNode);
    }

    public static String getValue(Node node) {
        String value = node.getNodeValue();
        if (value == null) {
            node = getChildByName(node, "#text");
            if (node != null)
                value = node.getNodeValue();
        }
        return value;
    }

    public static String getAttributeValue(Node node, String attributeName) {
        Node attributeNode = node.getAttributes().getNamedItem(attributeName);
        if (attributeNode == null)
            return null;
        else
            return attributeNode.getNodeValue();
    }

    public static String getValue(org.dom4j.Node node, String xpath) {
        if (node == null) {
            return null;
        }
        return node.selectSingleNode(xpath) == null ? null : node.selectSingleNode(xpath).getStringValue();
    }
}
