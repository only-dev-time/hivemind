"""Methods for normalizing steemd post metadata."""
#pylint: disable=line-too-long,too-many-lines

import re
import math
import ujson as json
import logging # for debugging TODO remove
from time import perf_counter # for debugging TODO remove
from funcy.seqs import first, distinct

from hive.utils.normalize import sbd_amount, rep_log10, safe_img_url, parse_time, utc_timestamp

log = logging.getLogger(__name__) # for debugging TODO remove

# TODO: remove this decorator after debugging
def performance_meter(func):
    """Decorator to measure performance of a function."""
    def wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = func(*args, **kwargs)
        end_time = perf_counter()
        log.debug("Function %s took %f seconds", func.__name__, end_time - start_time)
        return result
    return wrapper

def mentions(body):
    """Given a post body, return proper @-mentioned account names."""
    # condenser:
    # /(^|[^a-zA-Z0-9_!#$%&*@＠\/]|(^|[^a-zA-Z0-9_+~.-\/#]))[@＠]([a-z][-\.a-z\d]+[a-z\d])/gi,
    # twitter:
    # validMentionPrecedingChars = /(?:^|[^a-zA-Z0-9_!#$%&*@＠]|(?:^|[^a-zA-Z0-9_+~.-])(?:rt|RT|rT|Rt):?)/
    # endMentionMatch = regexSupplant(/^(?:#{atSigns}|[#{latinAccentChars}]|:\/\/)/);
    matches = re.findall(
        '(?:^|[^a-zA-Z0-9_!#$%&*@\\/])'
        '(?:@)'
        '([a-zA-Z0-9][a-zA-Z0-9\\-.]{1,14}[a-zA-Z0-9])'
        '(?![a-z])', body)
    return {grp.lower() for grp in matches}

def post_to_internal(post, post_id, level='insert', promoted=None):
    """Given a steemd post, build internal representation."""
    # pylint: disable=bad-whitespace

    #post['category'] = core['category']
    #post['community_id'] = core['community_id']
    #post['gray'] = core['is_muted']
    #post['hide'] = not core['is_valid']

    values = [('post_id', post_id)]

    # immutable; write only once (*edge case: undeleted posts)
    if level == 'insert':
        values.extend([
            ('author',   post['author']),
            ('permlink', post['permlink']),
            ('category', post['category']),
            ('depth',    post['depth'])])

    # always write, unless simple vote update
    if level in ['insert', 'payout', 'update']:
        basic = post_basic(post)
        values.extend([
            ('community_id',  post['community_id']), # immutable*
            ('created_at',    post['created']),    # immutable*
            ('updated_at',    post['last_update']),
            ('title',         post['title']),
            ('payout_at',     basic['payout_at']), # immutable*
            ('preview',       basic['preview']),
            ('body',          basic['body']),
            ('img_url',       basic['image']),
            ('is_nsfw',       basic['is_nsfw']),
            ('is_declined',   basic['is_payout_declined']),
            ('is_full_power', basic['is_full_power']),
            ('is_paidout',    basic['is_paidout']),
            ('json',          json.dumps(basic['json_metadata'])),
            ('raw_json',      json.dumps(post_legacy(post))),
        ])

    # if there's a pending promoted value to write, pull it out
    if promoted:
        values.append(('promoted', promoted))

    # update unconditionally
    payout = post_payout(post)
    stats = post_stats(post)
    
    # new scores with interaction
    # from hive.db.adapter import Db # TODO remove if checked, because fields not needed - here only for debugging
    # scores = post_scores(Db.instance(), post) # TODO remove if checked, because fields not needed - here only for debugging

    # //--
    # if community - override fields.
    # TODO: make conditional (date-based?)
    assert 'community_id' in post, 'comm_id not loaded'
    if post['community_id']:
        stats['hide'] = post['hide']
        stats['gray'] = post['gray']
    # //--

    values.extend([
        ('payout',      payout['payout']),
        ('rshares',     payout['rshares']),
        ('votes',       payout['csvotes']),
        # ('sc_trend',    payout['sc_trend']), # not needed for further processing
        # ('sc_hot',      payout['sc_hot']), # not needed for further processing
        ('flag_weight', stats['flag_weight']),
        ('total_votes', stats['total_votes']),
        ('up_votes',    stats['up_votes']),
        ('is_hidden',   stats['hide']),
        ('is_grayed',   stats['gray']),
        ('author_rep',  stats['author_rep']),
        ('children',    min(post['children'], 32767)),
    ])

    return values


def post_basic(post):
    """Basic post normalization: json-md, tags, and flags."""
    md = {}
    # At least one case where jsonMetadata was double-encoded: condenser#895
    # jsonMetadata = JSON.parse(jsonMetadata);
    try:
        md = json.loads(post['json_metadata'])
        if not isinstance(md, dict):
            md = {}
    except Exception:
        pass

    thumb_url = ''
    if md and 'image' in md:
        if md['image']:
            if not isinstance(md['image'], list):
                md['image'] = [md['image']]
            md['image'] = list(filter(None, map(safe_img_url, md['image'])))
        if md['image']:
            thumb_url = md['image'][0]
        else:
            del md['image']

    # clean up tags, check if nsfw
    tags = [post['category']]
    # if (typeof tags == 'string') tags = tags.split(' '); # legacy condenser compat
    if md and 'tags' in md and isinstance(md['tags'], list):
        tags = tags + md['tags']
    tags = map(lambda tag: (str(tag) or '').strip('# ').lower()[:32], tags)
    tags = filter(None, tags)
    tags = list(distinct(tags))[:5]
    is_nsfw = 'nsfw' in tags

    body = post['body']
    if body.find('\x00') > -1:
        #url = post['author'] + '/' + post['permlink']
        body = body.replace('\x00', '[NUL]')

    # payout date is last_payout if paid, and cashout_time if pending.
    is_paidout = (post['cashout_time'][0:4] == '1969')
    payout_at = post['last_payout'] if is_paidout else post['cashout_time']

    # payout is declined if max_payout = 0, or if 100% is burned
    is_payout_declined = False
    if sbd_amount(post['max_accepted_payout']) == 0:
        is_payout_declined = True
    elif len(post['beneficiaries']) == 1:
        benny = first(post['beneficiaries'])
        if benny['account'] == 'null' and int(benny['weight']) == 10000:
            is_payout_declined = True

    # payout entirely in SP
    is_full_power = int(post['percent_steem_dollars']) == 0

    return {
        'json_metadata': md,
        'image': thumb_url,
        'tags': tags,
        'is_nsfw': is_nsfw,
        'body': body,
        'preview': body[0:1024],

        'payout_at': payout_at,
        'is_paidout': is_paidout,
        'is_payout_declined': is_payout_declined,
        'is_full_power': is_full_power,
    }

def post_legacy(post):
    """Return legacy fields which may be useful to save.

    Some UI's may want to leverage these, but no point in indexing.
    """
    _legacy = ['id', 'url', 'root_comment', 'root_author', 'root_permlink',
               'root_title', 'parent_author', 'parent_permlink',
               'max_accepted_payout', 'percent_steem_dollars',
               'curator_payout_value', 'allow_replies', 'allow_votes',
               'allow_curation_rewards', 'beneficiaries']
    return {k: v for k, v in post.items() if k in _legacy}

def post_payout(post):
    """Get current vote/payout data and recalculate trend/hot score."""
    # total payout (completed and/or pending)
    payout = sum([
        sbd_amount(post['total_payout_value']),
        sbd_amount(post['curator_payout_value']),
        sbd_amount(post['pending_payout_value']),
    ])

    # `active_votes` was temporarily missing in dev -- ensure this condition
    # is caught ASAP. if no active_votes then rshares MUST be 0. ref: steem#2568
    assert post['active_votes'] or int(post['net_rshares']) == 0

    # get total rshares, and create comma-separated vote data blob
    rshares = sum(int(v['rshares']) for v in post['active_votes'])
    csvotes = "\n".join(map(_vote_csv_row, post['active_votes']))

    # _timestamp = utc_timestamp(parse_time(post['created'])) # not needed
    # sc_trend = _score(rshares, _timestamp, 240000) # calculate in post_scores
    # sc_hot = _score(rshares, _timestamp, 10000) # calculate in post_scores

    return {
        'payout': payout,
        'rshares': rshares,
        'csvotes': csvotes
        # 'sc_trend': sc_trend,
        # 'sc_hot': sc_hot 
    }

def _vote_csv_row(vote):
    """Convert a vote object into minimal CSV line."""
    rep = rep_log10(vote['reputation'])
    return "%s,%s,%s,%s" % (vote['voter'], vote['rshares'], vote['percent'], rep)

@performance_meter
def _score(rshares, created_timestamp, timescale=480000):
    """Calculate trending/hot score.

    Source: calculate_score - https://github.com/steemit/steem/blob/8cd5f688d75092298bcffaa48a543ed9b01447a6/libraries/plugins/tags/tags_plugin.cpp#L239
    """
    mod_score = rshares / 10000000.0
    order = math.log10(max((abs(mod_score), 1)))
    sign = 1 if mod_score > 0 else -1
    return sign * order + created_timestamp / timescale

def _weighted_rshares_linear_median_and_min(active_votes):
    """Calculate trending/hot score with linear median and min rshares."""
    if active_votes:
        rshares = [int(vote['rshares']) for vote in active_votes]
        length = len(rshares)
        median_rshares = sorted(rshares)[length // 2]
        median_rshares *= 1 if length > 10 else 0.00001 # under 10 votes scale down
        weighted_median_rshares = int(median_rshares * min(length / 30, 1))  # Linear weighted median
    else:
        weighted_median_rshares = 0
    return weighted_median_rshares

def post_stats(post):
    """Get post statistics and derived properties.

    Source: contentStats - https://github.com/steemit/condenser/blob/master/src/app/utils/StateFunctions.js#L109
    """
    neg_rshares = 0
    total_votes = 0
    up_votes = 0
    for vote in post['active_votes']:
        rshares = int(vote['rshares'])

        if rshares == 0:
            continue

        total_votes += 1
        if rshares > 0: up_votes += 1
        if rshares < 0: neg_rshares += rshares

    # take negative rshares, divide by 2, truncate 10 digits (plus neg sign),
    #   and count digits. creates a cheap log10, stake-based flag weight.
    #   result: 1 = approx $400 of downvoting stake; 2 = $4,000; etc
    flag_weight = max((len(str(int(neg_rshares / 2))) - 11, 0))

    author_rep = rep_log10(post['author_reputation'])
    has_pending_payout = sbd_amount(post['pending_payout_value']) >= 0.02

    return {
        'hide': author_rep < 0 and not has_pending_payout,
        'gray': author_rep < 1,
        'author_rep': author_rep,
        'flag_weight': flag_weight,
        'total_votes': total_votes,
        'up_votes': up_votes
    }

@performance_meter
def post_scores(db, post):
    """Get post score based on reblogged_by and replies."""

    # calculate score only for root posts
    if post['depth'] > 0:
        return {
            'sc_trend': 0.0,
            'sc_hot': 0.0
        }

    config = {
        'vote_weight': 0.2,            # Weight for original score components based on votes
        'interaction_weight': 0.7,     # Weight for interaction components
        'reblog_weight': 1.0,          # Resteems
        'comment_weight': 0.7,         # Children
        'reblog_divisor': 1.0,         # Resteem divisor for normalisation
        'comment_divisor': 2.0,        # Children divisor for normalisation
        'trending_timescale': 240000,  # 240k seconds = 66.67 hours
        'hot_factor': 24               # Hot score factor for timescale
    }

    pid = post['post_id']
    log.debug("POST_INTERACTION_SCORE: post: (%s) %s/%s", pid, post['author'], post['permlink']) # for debugging TODO remove

    # base score logic - old score
    created_timestamp = utc_timestamp(parse_time(post['created']))
    mod_score = int(post['net_rshares']) / 1e7
    log_order = math.log10(max(abs(mod_score), 1))
    sign = 1 if mod_score > 0 else -1
    votes_score = sign * log_order

    # interaction score component
    # TODO only for debugging
    # sql = "SELECT author,permlink FROM hive_posts WHERE id = :post_id"
    # post_row = db.query_row(sql, post_id=pid)
    # log.debug("POST_INTERACTION_SCORE: post_row: %s", post_row)
    # if post_row and ( post_row['author'] != post['author'] or post_row['permlink'] != post['permlink'] ):
    #     log.error("POST_INTERACTION_SCORE: post: %s/%s, db: %s/%s", post['author'], post['permlink'], post_row['author'], post_row['permlink'])

    # get reblog count
    sql = "SELECT COUNT(1) FROM hive_reblogs WHERE post_id = :post_id"
    reblogs = int(db.query_one(sql, post_id=pid)) #TODO in int umwandeln aber nicht direkt, da Coroutine Rückgabewert
    log.debug("POST_INTERACTION_SCORE: reblogs: %s", reblogs) # for debugging TODO remove
    # get children count
    children = post['children']
    log.debug("POST_INTERACTION_SCORE: children: %s", children) # for debugging TODO remove

    interaction_score = (
        config['reblog_weight'] * math.log10(max(reblogs / config['reblog_divisor'], 1)) +
        config['comment_weight'] * math.log10(max(children / config['comment_divisor'], 1))
    )

    # base score without time component
    base_score = votes_score * config['vote_weight'] + interaction_score * config['interaction_weight']
    log.debug("POST_INTERACTION_SCORE: base_score: %s, votes_score: %s, interaction_score: %s", base_score, votes_score, interaction_score) # for debugging TODO remove

    # time component
    trending_score = float(base_score + (created_timestamp / config['trending_timescale']))
    hot_score = float(base_score + (created_timestamp / (config['trending_timescale'] / config['hot_factor'])))
    log.debug("POST_INTERACTION_SCORE: trending: %s", trending_score) # for debugging TODO remove
    log.debug("POST_INTERACTION_SCORE: hot: %s", hot_score) # for debugging TODO remove

    return {
        'sc_trend': trending_score,
        'sc_hot': hot_score
    }
