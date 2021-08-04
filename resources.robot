*** Variables ***
${existingTopic}    tempTopic

*** Keywords ***
Verify Status Of Topic Creation
    [Arguments]    ${topic}
    ${DoesExist}    check if topic exists    ${topic}
    IF    ${DoesExist}==False
    create topic    ${topic}
    ${output}    check if topic exists    ${topic}
    Pass Execution    "${output}==True"
    ELSE IF    ${DoesExist}==True
    create topic    ${topic}

Verify Content Of Topic List Doesnt Change After Creation
    ${listBefore}    list all topics
    Run Keyword And Expect Error    STARTS:TopicAlreadyExistsError    create topic    ${existingTopic}
    ${listAfter}    list all topics
    Pass Execution    "${listBefore}==${listAfter}"

Verify Content Of Topic List Changes After Creation
    [Arguments]    ${newTopic}
    ${listBefore}    list all topics
    create topic    ${newTopic}
    ${listAfter}    list all topics
    Pass Execution    "${listBefore}!=${listAfter}"

Verify Content Of Topic List Changes After Deletion
    ${listBefore}    list all topics
    delete topic    ${existingTopic}
    ${listAfter}    list all topics
    Pass Execution    "${listBefore}!=${listAfter}"

Verify Content Of Topic List Doesnt Change After Deletion
    [Arguments]    ${newTopic}
    ${listBefore}    list all topics
    Run Keyword And Expect Error    STARTS:InvalidTopicError    delete topic    ${newTopic}
    ${listAfter}    list all topics
    Pass Execution    "${listBefore}==${listAfter}"
