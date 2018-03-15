/*
 * Copyright 2014-2017 the original author or authors.
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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import twitter4j.FilterQuery;
import twitter4j.RawStreamListener;
import twitter4j.TwitterStream;

import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Christian Tzolov
 */
public class Twitter4jStreamMessageProducer extends MessageProducerSupport {

	private TwitterStream twitterStream;
	private final TwitterStreamProperties twitterStreamProperties;

	private final Object monitor = new Object();

	private final AtomicBoolean running = new AtomicBoolean(false);

	protected Twitter4jStreamMessageProducer(TwitterStream twitterStream, TwitterStreamProperties twitterStreamProperties) {
		this.twitterStream = twitterStream;
		this.twitterStreamProperties = twitterStreamProperties;
		this.setPhase(Integer.MAX_VALUE);
	}

	@Override
	protected void doStart() {
		synchronized (this.monitor) {
			if (this.running.get()) {
				// already running
				return;
			}
			this.twitterStream.addListener(new RawStreamListener() {
				@Override
				public void onMessage(String rawString) {
					doSendLine(rawString);
				}

				@Override
				public void onException(Exception ex) {
					logger.error("Twitter streaming error!", ex);
				}
			});
			if (this.twitterStreamProperties.getStreamType() == TwitterStreamType.FILTER) {
				FilterQuery filterQuery = new FilterQuery();
				if (StringUtils.hasText(this.twitterStreamProperties.getFollow())) {
					long[] followAsLongArray = Arrays.stream(twitterStreamProperties.getFollow().split(","))
							.mapToLong(s -> Long.valueOf(s)).toArray();
					filterQuery.follow(followAsLongArray);
				}
				if (StringUtils.hasText(this.twitterStreamProperties.getTrack())) {
					filterQuery.track(this.twitterStreamProperties.getTrack());
				}

				if (StringUtils.hasText(this.twitterStreamProperties.getLocations())) {
					filterQuery.locations(toLocationsArray(this.twitterStreamProperties.getLocations()));
				}
				if (StringUtils.hasText(this.twitterStreamProperties.getLanguage())) {
					filterQuery.language(this.twitterStreamProperties.getLanguage());
				}

				filterQuery.count(twitterStreamProperties.getCount());

				this.twitterStream.filter(filterQuery);
			}
			else if (this.twitterStreamProperties.getStreamType() == TwitterStreamType.SAMPLE) {
				if (StringUtils.hasText(this.twitterStreamProperties.getLanguage())) {
					this.twitterStream.sample(this.twitterStreamProperties.getLanguage());
				}
				else {
					this.twitterStream.sample();
				}
			}
			else if (this.twitterStreamProperties.getStreamType() == TwitterStreamType.FIREHOSE) {
				this.twitterStream.firehose(twitterStreamProperties.getCount());
			}
			else {
				throw new IllegalArgumentException("Unsupported twitter API type:" + this.twitterStreamProperties.getStreamType());
			}


			this.running.set(true);
		}
	}

	private double[][] toLocationsArray(String locations) {
		String[] locStrArray = locations.split(",");
		Assert.isTrue(locStrArray.length == 0, "Incorrect locations attribute:" + locations);
		Assert.isTrue(locStrArray.length % 2 != 0, "Incorrect locations attribute:" + locations);

		double[][] locDoubleArray = new double[locStrArray.length / 2][2];
		for (int i = 0; i < locStrArray.length; i = i + 2) {
			locDoubleArray[i / 2][0] = Double.valueOf(locStrArray[i]);
			locDoubleArray[i / 2][1] = Double.valueOf(locStrArray[i + 1]);
		}

		return locDoubleArray;
	}

	@Override
	protected void doStop() {
		this.twitterStream.shutdown();
		this.running.set(false);
	}

	private static long count = 0;

	protected void doSendLine(String rawTweet) {
		if (rawTweet.startsWith("{\"limit")) {
			// discard
		}
		else if (rawTweet.startsWith("{\"delete")) {
			// discard
		}
		else if (rawTweet.startsWith("{\"warning")) {
			// discard
		}
		else if (StringUtils.hasText(rawTweet) == false) {
			// Keep-alive message
			// discard
		}
		else {
			logger.info("+tweet: " + count++ + ", size:" + rawTweet.length());
			sendMessage(MessageBuilder.withPayload(rawTweet).build());
		}
	}

}
