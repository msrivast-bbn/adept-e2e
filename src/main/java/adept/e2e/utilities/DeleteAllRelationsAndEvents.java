package adept.e2e.utilities;

/*-
 * #%L
 * adept-kb
 * %%
 * Copyright (C) 2012 - 2017 Raytheon BBN Technologies
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import adept.common.KBID;
import adept.kbapi.KB;
import adept.kbapi.KBConfigurationException;
import adept.kbapi.KBParameters;
import adept.kbapi.KBUpdateException;

import java.io.IOException;
import java.util.List;

/**
 * Utility that deletes only Relations and Events from a KB in case entity-dedup succeeded but relation/event upload or dedup failed for some reason.
 * This avoids clearing the whole KB and re-uploading everything to it from scratch.
 */
public class DeleteAllRelationsAndEvents {


    public static void main(String[] args) throws IOException,KBConfigurationException, KBUpdateException{
        if(args.length!=1){
            System.out.println("Expects path to a KBParameters file");
            System.exit(1);
        }
        System.out.println("Making connection to KB...");
        KB kb = new KB(new KBParameters(args[0]));
        System.out.println("Fetching KBIDs for all relations and events...");
        List<KBID> nonEntityKBIDs = kb.getKBIDsByType("adept-base:Relation",new String[] {});
        System.out.println("Deleting relations and events from the KB...");
        int size = nonEntityKBIDs.size();
        System.out.println("Total artifacts to delete: "+size);
        int count=0;
        for(KBID kbId : nonEntityKBIDs){
            kb.deleteKBObject(kbId);
            count++;
            if(count%1000==0){
                System.out.println("Deleted "+count+"/"+size+" relations/events so far...");
            }
        }
        System.out.println("Deleted a total of "+count+" out of "+size+" objects...");
        System.out.println("Done!");
    }
}
