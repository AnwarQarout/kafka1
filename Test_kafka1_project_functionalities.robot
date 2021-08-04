*** Settings ***
Resource          resources.robot
Library           TestKafka.py
Library           OperatingSystem

*** Test Cases ***
Create Topic That Already Exists
    Verify Content Of Topic List Doesnt Change After Creation

Create Topic That Doesnt Exist
    Verify Content Of Topic List Changes After Creation    anotherTopic

Delete Topic That Already Exists
    Verify Content Of Topic List Changes After Deletion

Delete Topic That Doesnt Exist
    Verify Content Of Topic List Doesnt Change After Deletion    aRandomTopic

Send File To Kafka And Ensure It Was Sent
    Send File To Kafka Topic    tempTopic    file.txt
    ${output}    kafka consumer    tempTopic    outfile.txt
    Should Not Be Empty    ${output}
    Should Contain    ${output}    third line

Assure Kafka Consumer Writes To Output File
    Kafka consumer    tempTopic    aFile.txt
    ${output}    Get File    aFile.txt
    Should Not Be Empty    ${output}
    Should Contain    ${output}    third line
