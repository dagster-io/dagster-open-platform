-- Test to ensure the final transcript JSONL arrays contain no duplicate sentences
-- This is a simpler, faster test that validates the final output

with transcript_data as (

    select 
        call_conversation_key,
        call_transcript_row_id,
        transcript_sentences
    from {{ ref('gong_call_transcripts') }}
    where transcript_sentences is not null
),

-- Flatten the JSONL arrays to check for duplicates within each transcript
flattened_final_sentences as (

    select 
        call_conversation_key,
        call_transcript_row_id,
        sentence.value:speaker_id::string as speaker_id,
        sentence.value:start_ms::integer as start_ms,
        sentence.value:text::string as sentence_text
    from transcript_data,
         lateral flatten(input => transcript_sentences) as sentence
),

-- Check for duplicates in the final output
duplicate_sentences as (

    select 
        call_conversation_key,
        call_transcript_row_id,
        speaker_id,
        start_ms,
        sentence_text,
        count(*) as occurrence_count
    from flattened_final_sentences
    group by 
        call_conversation_key,
        call_transcript_row_id,
        speaker_id,
        start_ms,
        sentence_text
    having count(*) > 1
)

-- This test fails if any duplicate sentences are found in the final arrays
select 
    call_conversation_key,
    call_transcript_row_id,
    speaker_id,
    start_ms,
    sentence_text,
    occurrence_count
from duplicate_sentences 