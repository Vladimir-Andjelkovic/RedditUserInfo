import configparser
import praw
import time
from db_class import DatabaseManager
from praw.exceptions import RedditAPIException


config = configparser.ConfigParser()
config.read('config.ini')

client_id = config['API']['client_id']
secret = config['API']['secret']
user_username = config['API']['username']
user_password = config['API']['password']
user_agent = f'script:SimpleApiApp:v1.0 (by u/{user_username})'

config_db_name = config['DATABASE']['db_name']
config_db_usernames_table_name = config['DATABASE']['username_table']

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=secret,
    password=user_password,
    username=user_username,
    user_agent=user_agent,
)


def get_from_specific_subreddit(subreddit_name, subreddit_counter):
    db_manager = DatabaseManager(config_db_name)
    # db_manager.create_table(config_db_usernames_table_name, 'username VARCHAR(100) UNIQUE')
    # db_manager.add_column_to_table()
    counter = 1

    try:
        check_and_wait()

        hot_posts = reddit.subreddit(subreddit_name).hot(limit=500)

        check_and_wait()

        for post in hot_posts:
            print(f'{subreddit_counter}.Subreddit: {subreddit_name}')
            print(f'Post - {post.title}')
            post.comments.replace_more(limit=None)
            comments = post.comments.list()
            for comment in comments:
                if comment.author:
                    author = comment.author.name
                    db_manager.add_record(config_db_usernames_table_name, ('username',), (author,))
            print(f'Comments: {len(comments)}')
            print(f'Done with post {counter}')
            print('----------------------------------------')
            counter += 1

    except RedditAPIException as e:
        if 'RATELIMIT' in str(e).upper():
            print('Rate limit exceeded. Waiting 60 seconds...')
            time.sleep(60)
        else:
            print(f'An error occurred: {e}')

    except Exception as e:
        time.sleep(60)
        print(f'Error: {e}')


def get_usernames_from_popular_subreddits():
    popular_subreddits = reddit.subreddit.popular(limit=10)
    sub_counter = 1

    for subreddit in popular_subreddits:
        get_from_specific_subreddit(subreddit.display_name, sub_counter)
        sub_counter += 1


def populate_user_info():
    db_manager = DatabaseManager(config_db_name)
    usernames = db_manager.get_usernames('users')
    counter = 0

    for username_tuple in usernames:
        try:

            if counter % 10 == 0:
                print(f'Finished with {counter} users.')

            check_and_wait()

            username = username_tuple[0]
            user = reddit.redditor(username)

            account_created_utc = user.created_utc
            comment_karma = user.comment_karma
            verified_email = user.has_verified_email
            reddit_user_id = user.id
            is_employee = user.is_employee
            is_gold = user.is_gold
            link_karma = user.link_karma

            if all(var is not None for var in [account_created_utc, comment_karma, verified_email, reddit_user_id,
                                               is_employee, is_gold, link_karma]):
                gone_through = True
            else:
                gone_through = False

            db_manager.add_user_info(account_created_utc, comment_karma, verified_email, reddit_user_id, is_employee,
                                     is_gold, link_karma, gone_through, username)

            counter += 1

        except RedditAPIException as e:
            if 'RATELIMIT' in str(e).upper():
                print('Rate limit exceeded. Waiting 60 seconds...')
                time.sleep(60)
            else:
                print(f'An error occurred: {e}')

        except Exception as e:
            print(f'Error: {e}')


def check_and_wait():
    remaining = reddit.auth.limits['remaining']
    reset_time = reddit.auth.limits['reset_timestamp']

    if remaining == 0:
        sleep_time = reset_time - time.time() + 5
        print(f"Rate limit reached, sleeping for {int(sleep_time)} seconds")
        time.sleep(sleep_time)


if __name__ == '__main__':
    start_time = time.time()

    # check_and_wait()
    # popular_subreddits = reddit.subreddits.popular(limit=100)
    # sub_counter = 1
    #
    # for subreddit in popular_subreddits:
    #     check_and_wait()
    #     subreddit_name = subreddit.display_name
    #     if subreddit_name:
    #         check_and_wait()
    #         get_from_specific_subreddit(subreddit_name, sub_counter)
    #         sub_counter += 1

    populate_user_info()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Elapsed Time: {elapsed_time:.2f} seconds')
