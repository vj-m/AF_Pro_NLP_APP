from flask import Flask, request, make_response, jsonify, Response
import re
from nlp import Data_ETL
import time
import json

nlp = Data_ETL()
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    return 'healthy'

@app.route("/api/search", methods=['POST'])
def search():
    st = time.time()
    data = request.get_json()
    data = json.loads(data)
    query = data['query']
    print('query - ', query)

    if query is None:
        make_response(jsonify({'error': 'Missing Query parameter'}), 400)
    else:
        search_response = nlp.search(query, N=3)
        '''        
        Response Output
        {
            'query' : query,
            'video_ids' : IDs
        }
        '''
        print('Time Taken for search(s) - ', time.time() - st)
        print(search_response)
        return Response(search_response, mimetype='application/json', status=200)


@app.route("/api/searchPro", methods=['POST'])
def searchPro():
    st = time.time()

    data = request.get_json()
    data = json.loads(data)
    query = data['query']
    print('query - ', query)

    if query is None:
        make_response(jsonify({'error': 'Missing Query parameter'}), 400)
    else:
        search_response = nlp.search_pro(query, N=3)
        '''        
        Response Output
        {

            'query' : query,
            'output' : [(id, score, thumbnail_id)]

        }
        '''
        print('Time Taken for search(s) - ', time.time() - st)
        print(search_response)
        return Response(search_response, mimetype='application/json', status=200)

@app.route("/api/add_support", methods=['POST'])
def add_support():
    st = time.time()
    data = request.get_json()
    # Ex. data = {'query': 'center field needs to attack', 'video_id': 2786}
    data = json.loads(data)
    support = data['query']
    vid_id = data['video_id']

    if support is None:
        make_response(jsonify({'error': 'Missing support parameter'}), 400)
    elif vid_id is None:
        make_response(jsonify({'error': 'Missing Video ID parameter'}), 400)
    else:
        nlp.add_support(vid_id, support)

    print('Time Taken for Add Support(s) - ', time.time() - st)
    return Response('added support successfully', mimetype='application/json', status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)