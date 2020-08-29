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

