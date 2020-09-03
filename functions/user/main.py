import json
from google.cloud import bigquery


def maximum_scores(user_id):
    client = bigquery.Client('speech-similarity')

    query_job = client.query(
        """
            SELECT OriginalVoiceId,
                   Max(score) as Score
            FROM   statistics.recorded_voices
            WHERE userid = '{userId}'
            GROUP  BY originalvoiceid
        """.format(userId=user_id)
    )

    maximum_scorers = query_job.result()
    results = {"maxScorers": []}

    for row in maximum_scorers:
        voice_id = row.OriginalVoiceId
        score = row.Score
        results["maxScorers"].append({"voiceId": voice_id, "score": score})

    return results
    # return {"maxScores": [{"voiceId": "123", "score": 0.8}, {"voiceId": "124", "score": 0.9}]}


def scores_by_time(user_id):
    client = bigquery.Client('speech-similarity')

    query_job = client.query(
        """
            select
              OriginalVoiceId,
              Score,
              Datetime,
              count
              (
                distinct(UserId)
              ) as tried
            from
              statistics.recorded_voices
            group by
              OriginalVoiceId
        """
    )
    return {"timeScores": [{"voiceId": "123", "scores": [dict(datetime="12.05.21", score=0.1), dict(datetime="13.05.21", score=0)]}]}


def popular_voices():
    # client = bigquery.Client('speech-similarity')
    #
    # query_job = client.query(
    #     """
    #         select
    #           OriginalVoiceId,
    #           count
    #           (
    #             distinct(UserId)
    #           ) as tried
    #         from
    #           statistics.recorded_voices
    #         group by
    #           OriginalVoiceId
    #     """
    # )
    return {"popular_voices": ["123", "124", "124"]}


def get_user_statistics(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    if request_json and 'userId' in request_json:
        voice_id = request_json['userId']
    elif request_args and 'userId' in request_args:
        voice_id = int(request_args['userId'])
    else:
        return ""
    return {**maximum_scores(), **scores_by_time(), **popular_voices()}
