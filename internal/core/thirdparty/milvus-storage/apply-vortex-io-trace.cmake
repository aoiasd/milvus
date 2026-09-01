execute_process(
    COMMAND git apply --check "${PATCH_FILE}"
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE apply_check)

if(apply_check EQUAL 0)
    execute_process(
        COMMAND git apply --whitespace=nowarn "${PATCH_FILE}"
        WORKING_DIRECTORY "${SOURCE_DIR}"
        RESULT_VARIABLE apply_result)
    if(NOT apply_result EQUAL 0)
        message(FATAL_ERROR "Failed to apply Vortex IO trace patch")
    endif()
    return()
endif()

execute_process(
    COMMAND git apply --reverse --check "${PATCH_FILE}"
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE reverse_check)
if(NOT reverse_check EQUAL 0)
    message(FATAL_ERROR
            "Vortex IO trace patch is neither applicable nor already applied")
endif()
