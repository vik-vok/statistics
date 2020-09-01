import json
from google.cloud import bigquery


def maximum_scores():
    return {"maxScores": [{"voiceId": "123", "score": 0.8}, {"voiceId": "124", "score": 0.9}]}


def scores_by_time():
    return {"timeScores": [{"voiceId": "123", "scores": [dict(datetime="12.05.21", score=0.1), dict(datetime="13.05.21", score=0)]}]}


def popular_voices():
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
