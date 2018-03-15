/*
 * Copyright 2015-2017 the original author or authors.
 *
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
 */

package org.springframework.cloud.stream.app.twitterstream.source;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.twitter.TwitterCredentials;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.MessageProducer;

/**
 * TwitterStream source app.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Christian Tzolov
 */
@EnableBinding(Source.class)
@EnableConfigurationProperties({ TwitterCredentials.class, TwitterStreamProperties.class })
public class TwitterstreamSourceConfiguration {

	@Autowired
	TwitterCredentials credentials;

	@Autowired
	TwitterStreamProperties twitterStreamProperties;

	@Autowired
	Source source;

	@Bean
	public MessageProducer messageProducer(TwitterStream twitterStream) {
		Twitter4jStreamMessageProducer messageProducer = new Twitter4jStreamMessageProducer(twitterStream, twitterStreamProperties);
		messageProducer.setOutputChannel(source.output());
		return messageProducer;
	}

	@Bean
	@ConditionalOnMissingBean
	public TwitterStream twitterStream() {
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.setOAuthConsumer(credentials.getConsumerKey(), credentials.getConsumerSecret());
		twitterStream.setOAuthAccessToken(new AccessToken(credentials.getAccessToken(), credentials.getAccessTokenSecret()));
		return twitterStream;
	}
}
