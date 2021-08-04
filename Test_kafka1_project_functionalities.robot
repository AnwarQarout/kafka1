*** Settings ***
Resource          resources.robot
Library           TestKafka.py
Library           OperatingSystem

*** Test Cases ***
Create Topic That Already Exists
    Verify Content Of Topic List Doesnt Change After Creation

Create Topic That Doesnt Exist
    Verify Content Of Topic List Changes After Creation    ${nonExistingTopic}

Delete Topic That Already Exists
    Verify Content Of Topic List Changes After Deletion

Delete Topic That Doesnt Exist
    Verify Content Of Topic List Doesnt Change After Deletion    ${nonExistingTopic}

Send File To Kafka And Ensure It Was Sent
    Send File To Kafka Topic    ${existingTopic}    ${existingFile}
    ${output}    kafka consumer    ${existingTopic}    ${outputF}
    Should Not Be Empty    ${output}
    Should Contain    ${output}    ${lineInFile}

Assure Kafka Consumer Writes To Output File
    Kafka consumer    ${existingTopic}    ${outputF}
    ${output}    Get File    ${outputF}
    Should Not Be Empty    ${output}
    Should Contain    ${output}    ${lineInFile}
