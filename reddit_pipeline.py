import praw, duckdb, json, random, time
from datetime import datetime, timezone

# API set-up
reddit = praw.Reddit(#old credentials, get new one unlocked fom support
    client_id="<REDACTED>",
    client_secret="<REDACTED>",
    user_agent="<REDACTED>"
)

# connecting to motherduck db
con = duckdb.connect("md:raw_reddit_listings")

# get test post
subreddit = reddit.subreddit("mechmarket")

CUTOFF = datetime(2024, 3, 1, tzinfo=timezone.utc)  # 2 years back from Mar 1, 2026
MAX_PULL = 50_000 # wont go this far, will need to do another approach for historical, STILL AVAIL?

for i, post in enumerate(subreddit.new(limit=MAX_PULL), start=1):

    created = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
    if created < CUTOFF:
        break

    con.execute("""
    INSERT INTO raw_posts (
      source, subreddit, post_id, permalink, url,
      title, body, author, created_utc,
      flair_text, num_comments, vote_score,
      raw_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
    ON CONFLICT(post_id) DO NOTHING 
    """, [
        "reddit",
        post.subreddit.display_name,
        post.id,
        post.permalink,
        post.url,
        post.title,
        post.selftext,
        str(post.author),
        created,
        post.link_flair_text,
        post.num_comments,
        post.score,
        json.dumps({
            "title": post.title,
            "body": post.selftext
        })
    ])

    print("Inserted:", post.title)

    # need this because reddit has been a pain in the butt and I keep getting flagged
    if i % 25 == 0:
        time.sleep(random.uniform(1.0, 3.0))
