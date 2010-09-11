///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Taesun Moon <tsunmoon@gmail.com>.
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.bayesian.converters;

import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.mathutils.TGMath;
import opennlp.textgrounder.bayesian.topostructs.*;
import org.jdom.Element;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class InternalSphericalV2ToXMLConverter extends InternalSphericalV1ToXMLConverter {

    /**
     *
     * @param _converterExperimentParameters
     */
    public InternalSphericalV2ToXMLConverter(
          ConverterExperimentParameters _converterExperimentParameters) {
        super(_converterExperimentParameters);
    }

    @Override
    protected void setTokenAttribute(Element _token, int _wordid, int _regid, int _coordid) {
        Coordinate coord = new Coordinate(TGMath.cartesianToGeographic(TGMath.normalizeVector(regionMeans[_regid])));
        _token.setAttribute("long", String.format("%.6f", coord.longitude));
        _token.setAttribute("lat", String.format("%.6f", coord.latitude));
    }
}
