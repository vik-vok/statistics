import json
from google.cloud import bigquery


def voice_average_score():
    client = bigquery.Client("speech-similarity")

    query_job = client.query(
        """
            select 
                OriginalVoiceId, AVG(Score) as avgg
            from 
                statistics.recorded_voices 
            group by
                OriginalVoiceId
        """
    )

    average_scores = query_job.result()

    result = {}

    for row in average_scores:
        result[row.OriginalVoiceId] = {"avgScore": row.avgg}
    # print("yeeee")
    # print(result)
    return result


def voice_maximum_scorers():
    # TOP = 3
    client = bigquery.Client("speech-similarity")

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
                statistics.recorded_voices
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
            result[orig] = {"maxScorers": []}
        result[orig]["maxScorers"].append(
            {"UserId": row.UserId, "Score": row.Score,}
        )

    return result


def voice_users_tried():
    client = bigquery.Client("speech-similarity")

    query_job = client.query(
        """
            select
              OriginalVoiceId,
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

    users_tried = query_job.result()
    results = {}

    for row in users_tried:
        results[row.OriginalVoiceId] = {"numberTried": row.tried}

    return results


def get_all_voice_statistics(request):
    users_tried = voice_users_tried()
    average_scores = voice_average_score()
    max_scorers = voice_maximum_scorers()

    print(users_tried)
    print(average_scores)
    print(max_scorers)

    result = {}
    for voice_id in users_tried.keys():
        statistics = {
            **users_tried[voice_id],
            **average_scores[voice_id],
            **max_scorers[voice_id],
        }
        result[voice_id] = statistics

    return json.dumps(result)


def get_one_voice_statistics(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    if request_json and "originalVoiceId" in request_json:
        voiceId = request_json["originalVoiceId"]
    elif request_args and "originalVoiceId" in request_args:
        voiceId = request_args["originalVoiceId"]
    else:
        return (json.dumps({"error": "Missing parameter: originalVoiceId"}), 422, {})

    result = {}
    result.update(voice_users_tried()[str(voiceId)])
    result.update(voice_average_score()[str(voiceId)])
    result.update(voice_maximum_scorers()[str(voiceId)])

    return json.dumps(result)


# if __name__ == '__main__':
#     print(voice_average_score())
