import os
import io
import re
import csv
import zipfile
import requests
import sys
from hdbcli import dbapi
import hdbcli
import json
from flask import Flask
app = Flask(__name__)

# TODO: dynamically assign varchar
# TODO: assign integer,specific types to possible integer replies etc
# TODO: if one sentiment column all data are empty, do not include it
# Only need to change Username, password to db, and qualtric survey and API Key
db_username = "XXXXXXXXXXXXX"  #HANACLoud DB User
db_password = "XXXXXXXXXXXXX"  #HANA Cloud Password
qtrics_surv_id = "XXXXXXXXXXXXX"  #Provide your Survey ID
qtrics_api_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"  #Provide your API Token
schema_str = "BTP"  # replace it with another schema
###
FILE_NAME = ""


def getReponse(dataCenter, apiToken, surveyId):
    headers = {
        "content-type": "application/json",
        "x-api-token": apiToken,
    }

    url = "https://{0}.qualtrics.com/API/v3/surveys/{1}/response-schema".format(
        dataCenter, surveyId)
    downloadRequestUrl = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(
        dataCenter, surveyId)
    progressStatus = "inProgress"
    rsp = requests.request("GET", url, headers=headers)
    global FILE_NAME
    FILE_NAME = rsp.json()["result"]["title"].split("(")[0].strip()

    downloadRequestPayload = '{"format":"' + "csv" + '"}'
    downloadRequestResponse = requests.request(
        "POST", downloadRequestUrl, data=downloadRequestPayload, headers=headers)
    progressId = downloadRequestResponse.json()["result"]["progressId"]
    print(downloadRequestResponse.text)

    # Step 2: Checking on Data Export Progress and waiting until export is ready
    while progressStatus != "complete" and progressStatus != "failed":
        print("progressStatus=", progressStatus)
        requestCheckUrl = downloadRequestUrl + progressId
        requestCheckResponse = requests.request(
            "GET", requestCheckUrl, headers=headers)
        requestCheckProgress = requestCheckResponse.json()[
            "result"]["percentComplete"]
        print("Download is " + str(requestCheckProgress) + " complete")
        progressStatus = requestCheckResponse.json()["result"]["status"]

    # step 2.1: Check for error
    if progressStatus == "failed":
        raise Exception("export failed")

    fileId = requestCheckResponse.json()["result"]["fileId"]

    # Step 3: Downloading file, return data
    requestDownloadUrl = downloadRequestUrl + fileId + '/file'
    requestDownload = requests.request(
        "GET", requestDownloadUrl, headers=headers, stream=True)
    result_data = zipfile.ZipFile(io.BytesIO(requestDownload.content))
    result_data = {name: result_data.read(name) for name in result_data.namelist()}[
        FILE_NAME+".csv"].decode("utf-8").split("\r\n")
    for i in range(len(result_data)):
        sub_str1 = re.search('".*"', result_data[i])
        if sub_str1 != None:
            result_data[i] = result_data[i].replace(
                sub_str1.group(0), "".join(sub_str1.group(0).split(",")))
            result_data[i] = "".join(result_data[i].split('"'))
        result_data[i] = result_data[i].split(",")
    result_data.pop(-1)
    print('File has been successfully downloaded from qualtrics, please wait for data to be uploaded to Hana')
    return result_data


def pydbConnect(file_data):
    # setting table column headers NOTE:Column names for qualtrics survey may change in the filter. Not all columns are included.
    format_file_name = FILE_NAME.replace(" ", "_").replace("-", "_")
    format_file_name = re.sub("_+", "_", format_file_name)
    q_table_str = format_file_name + "_" + "QUESTIONS"
    usr_table_str = format_file_name + "_" + "USERINFO"
    q_id_str = "QUESTION_ID"
    q_content_str = "QUESTION_CONTENT"
    r_table_str = "RESPONSES"
    r_content_str = "RESPONSE_CONTENT"
    r_id_str = "RESPONSEID"
    r_id_num = 1
    # connect to hdb
    connection = dbapi.connect(
        address="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",  #HANA Cloud host
        port=443,
        user=db_username,
        password=db_password
    )
    cursor = connection.cursor()
    row_index = 0
    first_row = []
    uInfo_col_headers = ""
    uInfo_header_with_def = ""
    for row in file_data:
        row = row[5:6] + row[8:12] + row[17:]
        if row_index == 0:  # store first row that contains the column headers
            first_row = row
        elif row_index == 1:  # Initializing questions/userinfo table
            uInfo_data = ""
            isQuestionCol = False
            isSolutionRevisNull = True
            for i in range(len(first_row)):
                if first_row[i] == "SolutionRevision":
                    isSolutionRevisNull = False
            for i in range(len(row)):
                row[i] = row[i].split("(")[0]
                row[i] = row[i].replace("-", "_")
                row[i] = re.sub("_+", "_", row[i])
                first_row[i] = first_row[i].replace("(", "").replace(")", "")
                first_row[i] = first_row[i].replace(" ", "_")
                first_row[i] = first_row[i].replace("-", "_")
                first_row[i] = re.sub("_+", "_", first_row[i])
                uInfo_header_with_def += ("%s NVARCHAR(500)," % first_row[i])
                uInfo_col_headers += ("%s," % first_row[i])
                if first_row[i] == "ResponseId":
                    uInfo_data += ("%s NVARCHAR(500) PRIMARY KEY" % row[i])
                else:
                    uInfo_data += ("%s NVARCHAR(500)," % row[i])
                if isQuestionCol:  # creating, updating inserting into question table
                    uInfo_data += ("%s NVARCHAR(500)," % first_row[i])
                    try:
                        cursor.execute("SELECT * FROM %s.%s" %
                                       (schema_str, q_table_str))
                    except:
                        cursor.execute("CREATE COLUMN TABLE %s.%s (%s NVARCHAR(15) PRIMARY KEY,%s NVARCHAR(500))" % (
                            schema_str, q_table_str, q_id_str, q_content_str))
                    try:
                        cursor.execute("INSERT INTO %s.%s  (%s, %s)  VALUES(\'%s\',\'%s\')" % (
                            schema_str, q_table_str, q_id_str, q_content_str, first_row[i], row[i]))
                    except:
                        cursor.execute("UPDATE %s.%s SET %s = '%s' where %s = '%s'" % (
                            schema_str, q_table_str, q_content_str, row[i], q_id_str, first_row[i]))
                if first_row[i] == "RecipientEmail":
                    isQuestionCol = True
                # if there exist SolutionRevision column or it's the last column, create userinfo table here
                if (i == len(row) - 1 and isSolutionRevisNull) or i == len(row) - 1:
                    try:
                        cursor.execute('SELECT * FROM %s.%s' %
                                       (schema_str, usr_table_str))
                    except:
                        uInfo_data = uInfo_data[0:len(uInfo_data) - 1]
                        uInfo_header_with_def = uInfo_header_with_def[0:len(
                            uInfo_header_with_def) - 1]
                        cursor.execute('CREATE COLUMN TABLE %s.%s (%s)' % (
                            schema_str, usr_table_str, uInfo_header_with_def))
        elif row_index > 2:  # starting 4th row are the data
            uInfo_data = ""
            uInfo_header_list = uInfo_col_headers.split(",")
            isNewData = True
            for i in range(len(row)):
                row[i] = row[i].replace("'", "")
                uInfo_data += ("\'%s\'," % row[i])
                cursor2 = connection.cursor()
                cursor2.execute("SELECT * FROM %s.%s where %s = '%s'" %
                                (schema_str, usr_table_str, r_id_str, row[r_id_num]))
                for data in cursor2:
                    isNewData = False
                    break
            uInfo_data = uInfo_data[0:len(uInfo_data) - 1]
            if uInfo_col_headers[-1] == ",":
                uInfo_col_headers = uInfo_col_headers[0:len(
                    uInfo_col_headers) - 1]
            if isNewData:
                cursor.execute("INSERT INTO %s.%s (%s) VALUES(%s)" % (
                    schema_str, usr_table_str,  uInfo_col_headers, uInfo_data))
            else:
                cursor.execute("UPDATE %s.%s SET (%s) = (%s) where %s = '%s'" % (
                    schema_str, usr_table_str, uInfo_col_headers, uInfo_data, r_id_str, row[r_id_num]))
        row_index += 1
    print("data has been successfully loaded into hana")
    connection.close()


@app.route('/')
def main():
    print('Welcome to Qualtrics Response Extract')
    # token needs to be stored in os_env
    file_data = getReponse("XXX", qtrics_api_key, qtrics_surv_id)   #Proivde your Qualtrics data denter
    pydbConnect(file_data)
    return "Survey response has been succesfully replicated into HANA Cloud"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
