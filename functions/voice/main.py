import json
from .queries import queries


def get_all_voice_statistics(request):
    users_tried = queries.voice_users_tried()
    average_scores = queries.voice_average_score()
    max_scorers = queries.voice_maximum_scorers()

    result = {}

    for voice_id in users_tried.keys():
        statistics = {'usersTried': users_tried[voice_id],
                      'averageScorers': average_scores[voice_id],
                      'maxScorers': max_scorers[voice_id]}
        result['originalVoiceId'] = statistics

    return json.dumps(result)


def get_one_voice_statistics(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    if request_json and 'originalVoiceId' in request_json:
        voice_id = request_json['originalVoiceId']
    elif request_args and 'originalVoiceId' in request_args:
        voice_id = int(request_args['originalVoiceId'])
    else:
        # return error apiresponse
        return ""

    result = {}
    result.update(queries.voice_users_tried()[voice_id])
    result.update(queries.voice_average_score()[voice_id])
    result.update(queries.voice_maximum_scorers()[voice_id])

    return json.dumps(result)

