from enum import Enum

from enum import Enum

# Centralize the bucket names
BASE_BUCKETS = [
    # gisib has no match with a BGT object
    ("BUCKET0", "no_matches"),
    # gisib and BGT have a 1:1 relationship without other intersections with overlap above the set threshhold
    ("BUCKET1", "geom_1_to_1"),
    # merge gisib objects back to the geometry of BGT because of same attributes for the gisib objects
    # BGT is more detailed than gisib, 1 object in gisib relates to N objects in gisib - split BGT
    ("BUCKET2", "bgt_split"),
    ("BUCKET3", "gisib_merge"),
    # gisib is more detailed than BGT - N:1, split the BGT objects
    ("BUCKET4", "gisib_split"),
    # after these matches, a check is done to see if a gisib object has a main match with a BGT object above 75% both sides
    ("BUCKET5", "geom_75_match"),
    # a smaller gisib object in a larger BGT object. gisib <= 50% size of BGT object with a overlap of at least 85%
    ("BUCKET6", "clip_match"),
    # Others
    ("REMAINING", "remaining"),
]

VRH_BUCKETS = BASE_BUCKETS[:-2] + [  # Remove last 2 from BASE_BUCKETS
    ("BUCKET6", "geom_overlap_150_match"),
    ("BUCKET7", "clip_match"),
    ("REMAINING", "remaining"),
]

# Factory to create enums
def create_enum(name, items):
    return Enum(name, {k: v for k, v in items})

BucketsBase = create_enum("BucketsBase", BASE_BUCKETS)
BucketsVRH = create_enum("BucketsVRH", VRH_BUCKETS)

AUTOMATIC_BUCKETS = [
    BucketsBase.BUCKET1,
    BucketsBase.BUCKET4,
    BucketsBase.BUCKET5,
]

AUTOMATIC_BUCKETS_VRH = AUTOMATIC_BUCKETS.append(BucketsVRH.BUCKET6)