import json
from google.cloud import bigquery


def voice_average_score():
    client = bigquery.Client('speech-similarity')

    query_job = client.query(
        """
            select 
                OriginalVoiceId, AVG(Score) as avg
            from 
                statistics.test 
            group by
                OriginalVoiceId
        """
    )

    average_scores = query_job.result()

    result = {}

    for row in average_scores:
        result[row.OriginalVoiceId] = row.avg

    return json.dumps(result)


def voice_maximum_scorers():
    # TOP = 3
    client = bigquery.Client('speech-similarity')

    query_job = client.query(
        """
            with
              cte 
            as
            ( 
              select 
                OriginalVoiceId, UserId, Score, ROW_NUMBER() 
              over 
              (
                partition by 
                  OriginalVoiceId
                order by 
                  Score 
                desc
              )
              as 
                rn
              from 
                statistics.test
            )
            select 
              OriginalVoiceId, UserId, Score, rn
            from 
              cte
            where 
              rn <= 3
            order by 
              OriginalVoiceId, rn 
        """

    )

    max_scorers = query_job.result()
    result = {}

    for row in max_scorers:
        orig = row.OriginalVoiceId
        if orig not in result:
            result[orig] = []
        result[orig].append({
            'UserId' : row.UserId,
            'Score': row.Score,
        })

    return json.dumps(result)


def voice_users_tried():
    client = bigquery.Client('speech-similarity')

    query_job = client.query(
        """
            select
              OriginalVoiceId,
              count
              ( 
                distinct(UserId)
              ) as tried
            from 
              statistics.test
            group by
              OriginalVoiceId
        """
    )

    users_tried = query_job.result()
    results = {}

    for row in users_tried:
        results[row.OriginalVoiceId] = row.tried

    return json.dumps(results)
