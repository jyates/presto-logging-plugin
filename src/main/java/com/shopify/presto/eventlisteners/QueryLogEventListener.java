/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shopify.presto.eventlisteners;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import io.airlift.log.Logger;

/**
 * Created by jackmccracken on 2017-02-09.
 */
public class QueryLogEventListener implements EventListener
{
    private static final Logger log = Logger.get(QueryLogEventListener.class);

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        log.info("QID " + queryCompletedEvent.getMetadata().getQueryId() + " text " + queryCompletedEvent.getMetadata().getQuery());
        log.info("QID " + queryCompletedEvent.getMetadata().getQueryId() + " cpu time" + queryCompletedEvent.getStatistics().getCpuTime().getSeconds()/60 + " wall time (minutes) " + queryCompletedEvent.getStatistics().getWallTime().getSeconds()/60.0);
    }
}
