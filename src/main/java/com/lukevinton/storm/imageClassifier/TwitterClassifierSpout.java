/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package com.lukevinton.storm.imageClassifier;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import twitter4j.*;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author davidk
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterClassifierSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		UserStreamListener uListener = new UserStreamListener() {
			@Override
			public void onDeletionNotice(long l, long l1) {

			}

			@Override
			public void onFriendList(long[] longs) {

			}

			@Override
			public void onFavorite(User user, User user1, Status status) {

			}

			@Override
			public void onUnfavorite(User user, User user1, Status status) {

			}

			@Override
			public void onFollow(User user, User user1) {

			}

			@Override
			public void onDirectMessage(DirectMessage directMessage) {

			}

			@Override
			public void onUserListMemberAddition(User user, User user1, UserList userList) {

			}

			@Override
			public void onUserListMemberDeletion(User user, User user1, UserList userList) {

			}

			@Override
			public void onUserListSubscription(User user, User user1, UserList userList) {

			}

			@Override
			public void onUserListUnsubscription(User user, User user1, UserList userList) {

			}

			@Override
			public void onUserListCreation(User user, UserList userList) {

			}

			@Override
			public void onUserListUpdate(User user, UserList userList) {

			}

			@Override
			public void onUserListDeletion(User user, UserList userList) {

			}

			@Override
			public void onUserProfileUpdate(User user) {

			}

			@Override
			public void onBlock(User user, User user1) {

			}

			@Override
			public void onUnblock(User user, User user1) {

			}

			@Override
			public void onStatus(Status status) {

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

			}

			@Override
			public void onTrackLimitationNotice(int i) {

			}

			@Override
			public void onScrubGeo(long l, long l1) {

			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {

			}

			@Override
			public void onException(Exception e) {

			}
		};

		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

            @Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            @Override
			public void onException(Exception e) {
			}
		};

        TwitterStreamFactory factory = new TwitterStreamFactory();
		twitterStream = factory.getInstance();
		twitterStream.addListener(listener);
		twitterStream.addListener(uListener);
		String[] users = {"imageclassifier"};
		twitterStream.user(users);
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
        } else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
