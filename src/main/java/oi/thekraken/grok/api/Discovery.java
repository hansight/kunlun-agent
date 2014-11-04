/*******************************************************************************
 * Copyright 2014 Anthony Corbacho and contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package oi.thekraken.grok.api;

import com.google.code.regexp.Matcher;
import com.google.code.regexp.Pattern;
import oi.thekraken.grok.api.exception.GrokException;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.Map.Entry;

/**
 * {@code Discovery} try to find the best pattern for the given string.
 *
 * @author anthonycorbacho
 * @since 0.0.2
 */
public class Discovery {

    /**
     * Sort by regex complexity.
     *
     * @param groks of the pattern name and grok instance
     * @return the map sorted by grok pattern complexity
     */
    private Map<String, Grok> sort(Map<String, Grok> groks) {

        List<Grok> list = new ArrayList<>(groks.values());
        Map<String, Grok> map = new LinkedHashMap<>();
        Collections.sort(list, new Comparator<Grok>() {

            public int compare(Grok g1, Grok g2) {
                return (this.complexity(g1.getNamedRegex()) < this.complexity(g2.getNamedRegex())) ? 1
                        : 0;
            }

            private int complexity(String expandedPattern) {
                int score = 0;
                score += expandedPattern.split("\\Q" + "|" + "\\E", -1).length - 1;
                score += expandedPattern.length();
                return score;
            }
        });

        for (Grok g : list) {
            map.put(g.getSavedPattern(), g);
        }
        return map;

    }

    /**
     * @param expandedPattern string
     * @return the complexity of the regex
     */
    private int complexity(String expandedPattern) {
        int score = 0;
        score += expandedPattern.split("\\Q" + "|" + "\\E", -1).length - 1;
        score += expandedPattern.length();
        return score;
    }

    /**
     * Find a pattern from a log.
     *
     * @param text witch is the representation of your single
     * @return Grok pattern %{Foo}...
     */
    public String discover(String text,Grok grok) {
        if (text == null) {
            return "";
        }

        Map<String, Grok> groks = new TreeMap<>();
        Map<String, String> gPatterns = grok.getPatterns();
        // Boolean done = false;
        String texte = text;

        // Compile the pattern
        for (Entry<String, String> pairs : gPatterns.entrySet()) {
            String key = pairs.getKey();
            Grok g = new Grok();
            try {
                g.copyPatterns(gPatterns);
                g.setSavedPattern(key);
                g.compile("%{" + key + "}");
                groks.put(key, g);
            } catch (GrokException e) {
                //TODO Add logger
            }

        }

        // Sort patterns by complexity
        Map<String, Grok> patterns = this.sort(groks);

        // while (!done){
        // done = true;
        for (Entry<String, Grok> pairs : patterns.entrySet()) {
            String key = pairs.getKey();
            Grok value = pairs.getValue();

            // We want to search with more complex pattern
            // We avoid word, small number, space....
            if (this.complexity(value.getNamedRegex()) < 20) {
                continue;
            }

            Match m = value.match(text);
            if (m.isNull()) {
                continue;
            }
            // get the part of the matched text
            String part = getPart(m, text);

            // we skip boundary word
            Pattern pattern = Pattern.compile(".\\b.");
            Matcher ma = pattern.matcher(part);
            if (!ma.find()) {
                continue;
            }

            // We skip the part that already include %{Foo}
            Pattern pattern2 = Pattern.compile("%\\{[^}+]\\}");
            Matcher ma2 = pattern2.matcher(part);

            if (ma2.find()) {
                continue;
            }
            texte = StringUtils.replace(texte, part, "%{" + key + "}");
        }
        // }

        return texte;
    }

    /**
     * Get the substring that match with the text.
     *
     * @param m    Match
     * @param text
     * @return string
     */
    private String getPart(Match m, String text) {

        if (m == null || text == null) {
            return "";
        }

        return text.substring(m.getStart(), m.getEnd());
    }
}
