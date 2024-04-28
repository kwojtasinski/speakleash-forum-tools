#!/usr/bin/env python
# coding: utf-8

# <div class="align-center">
#   <a href="https://github.com/unslothai/unsloth"><img src="https://github.com/unslothai/unsloth/raw/main/images/unsloth%20new%20logo.png" width="110"></a>
#   <a href="https://discord.gg/u54VK8m8tk"><img src="https://github.com/unslothai/unsloth/raw/main/images/Discord.png" width="150"></a>
#   <a href="https://huggingface.co/docs/trl/main/en/index"><img src="https://github.com/huggingface/blog/blob/main/assets/133_trl_peft/thumbnail.png?raw=true" width="100"></a>
# </div>
# 
# To install Unsloth on your own computer, follow the installation instructions on our Github page [here](https://github.com/unslothai/unsloth#installation-instructions---conda).
# 
# You will learn how to do [data prep](#Data), how to [train](#Train), how to [run the model](#Inference), & [how to save it](#Save) (eg for Llama.cpp).

# In[1]:


get_ipython().system('yes | python -m pip install wandb --upgrade')
get_ipython().system('yes | python -m pip install --upgrade datasets')
get_ipython().system('yes | python -m pip install torch==2.1.1 --index-url https://download.pytorch.org/whl/cu118')
get_ipython().system('python -m pip install xformers gradio')

### ONLY on RTX A4000++
get_ipython().system('python -m pip install "unsloth[cu118_ampere_torch211] @ git+https://github.com/unslothai/unsloth.git"')
get_ipython().system('python -m pip install "git+https://github.com/huggingface/transformers.git" # Native 4bit loading works!')


# In[1]:


import torch; 
print(f"CUDA: {torch.version.cuda}")
print(f"PyTorch: {torch.__version__}")

# https://pytorch.org/get-started/locally/
# https://github.com/unslothai/unsloth#installation-instructions---pip


# In[2]:


import torch
major_version, minor_version = torch.cuda.get_device_capability()
print(f"CUDA Device Capability: {major_version}.{minor_version}")


# * We support Llama, Mistral, CodeLlama, TinyLlama, Vicuna, Open Hermes etc
# * And Yi, Qwen ([llamafied](https://huggingface.co/models?sort=trending&search=qwen+llama)), Deepseek, all Llama, Mistral derived archs.
# * We support 16bit LoRA or 4bit QLoRA. Both 2x faster.
# * `max_seq_length` can be set to anything, since we do automatic RoPE Scaling via [kaiokendev's](https://kaiokendev.github.io/til) method.
# * [**NEW**] With [PR 26037](https://github.com/huggingface/transformers/pull/26037), we support downloading 4bit models **4x faster**! [Our repo](https://huggingface.co/unsloth) has Llama, Mistral 4bit models.

# In[3]:


model_name = "Azurro/APT3-1B-Base"
max_seq_length = 2048   # Choose any! We auto support RoPE Scaling internally!
learning_rate = 2e-4
weight_decay = 0.01
max_steps = 12000
epoch = 4
warmup_steps = 10
batch_size = 4
gradient_accumulation_steps = 4
lr_scheduler_type = "linear"
optimizer = "adamw_8bit"
use_gradient_checkpointing = True
random_state = 3407
output_dirro_checkpoints = "output_check_APT3-1B_v2_4epoch"


# In[4]:


from unsloth import FastLanguageModel
import torch
dtype = None # None for auto detection. Float16 for Tesla T4, V100, Bfloat16 for Ampere+
load_in_4bit = False # Use 4bit quantization to reduce memory usage. Can be False.

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name = model_name,
    max_seq_length = max_seq_length,
    dtype = dtype,
    load_in_4bit = load_in_4bit,
    # token = "hf_...", # use one if using gated models like meta-llama/Llama-2-7b-hf
)


# We now add LoRA adapters so we only need to update 1 to 10% of all parameters!

# In[5]:


model = FastLanguageModel.get_peft_model(
    model,
    r = 16, # Choose any number > 0 ! Suggested 8, 16, 32, 64, 128
    target_modules = ["q_proj", "k_proj", "v_proj", "o_proj",
                      "gate_proj", "up_proj", "down_proj",],
    lora_alpha = 16,
    lora_dropout = 0, # Currently only supports dropout = 0
    bias = "none",    # Currently only supports bias = "none"
    use_gradient_checkpointing = True,
    random_state = 3407,
    max_seq_length = max_seq_length,
)


# <a name="Data"></a>
# ### Data Prep
# We now use the Alpaca dataset from [yahma](https://huggingface.co/datasets/yahma/alpaca-cleaned), which is a filtered version of 52K of the original [Alpaca dataset](https://crfm.stanford.edu/2023/03/13/alpaca.html). You can replace this code section with your own data prep.
# 
# **[NOTE]** To train only on completions (ignoring the user's input) read TRL's docs [here](https://huggingface.co/docs/trl/sft_trainer#train-on-completions-only).

# In[6]:


#@title Alpaca dataset preparation code
alpaca_prompt = """Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

### Instruction:
{}

### Input:
{}

### Response:
{}"""

def formatting_prompts_func(examples):
    instructs = examples["instruct"]
    inputs = examples["input"]
    outputs = examples["output"]
    sources = examples["source"]
    texts = []
    for instruct, input, output, source in zip(instructs, inputs, outputs, sources):
        text = alpaca_prompt.format(instruct, input, output, source)
        texts.append(text)
    return { "text" : texts, }
pass

from datasets import load_dataset
dataset = load_dataset('json', data_files="speakleash_pl_instructions_v0_0_1_ok.json", split = "train")
dataset = dataset.map(formatting_prompts_func, batched = True,)


# In[5]:


alpaca_prompt = """Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

### Instruction:
{}

### Input:
{}

### Response:
{}"""


# In[7]:


dataset


# <a name="Train"></a>
# ### Train the model
# Now let's use Huggingface TRL's `SFTTrainer`! More docs here: [TRL SFT docs](https://huggingface.co/docs/trl/sft_trainer). We do 60 steps to speed things up, but you can set `num_train_epochs=1` for a full run, and turn off `max_steps=None`. We also support TRL's `DPOTrainer`!

# In[8]:


from trl import SFTTrainer
from transformers import TrainingArguments

trainer = SFTTrainer(
    model = model,
    train_dataset = dataset,
    dataset_text_field = "text",
    max_seq_length = max_seq_length,
    args = TrainingArguments(
        per_device_train_batch_size = batch_size,
        gradient_accumulation_steps = gradient_accumulation_steps,
        warmup_steps = warmup_steps,
        num_train_epochs = epoch,
        # max_steps = max_steps,
        learning_rate = learning_rate,
        fp16 = not torch.cuda.is_bf16_supported(),
        bf16 = torch.cuda.is_bf16_supported(),
        logging_steps = 1,
        optim = "adamw_8bit",
        weight_decay = 0.01,
        lr_scheduler_type = "linear",
        seed = 3407,
        output_dir = output_dirro_checkpoints,
    ),
)


# In[9]:


#@title Show current memory stats
gpu_stats = torch.cuda.get_device_properties(0)
start_gpu_memory = round(torch.cuda.max_memory_reserved() / 1024 / 1024 / 1024, 3)
max_memory = round(gpu_stats.total_memory / 1024 / 1024 / 1024, 3)
print(f"GPU = {gpu_stats.name}. Max memory = {max_memory} GB.")
print(f"{start_gpu_memory} GB of memory reserved.")


# In[ ]:


### TRENING
trainer_stats = trainer.train(resume_from_checkpoint=True)     # resume_from_checkpoint=True


# In[ ]:


#@title Show final memory and time stats
used_memory = round(torch.cuda.max_memory_reserved() / 1024 / 1024 / 1024, 3)
used_memory_for_lora = round(used_memory - start_gpu_memory, 3)
used_percentage = round(used_memory         /max_memory*100, 3)
lora_percentage = round(used_memory_for_lora/max_memory*100, 3)
print(f"{trainer_stats.metrics['train_runtime']} seconds used for training.")
print(f"{round(trainer_stats.metrics['train_runtime']/60, 2)} minutes used for training.")
print(f"Peak reserved memory = {used_memory} GB.")
print(f"Peak reserved memory for training = {used_memory_for_lora} GB.")
print(f"Peak reserved memory % of max memory = {used_percentage} %.")
print(f"Peak reserved memory for training % of max memory = {lora_percentage} %.")


# <a name="Inference"></a>
# ### Inference
# Let's run the model! You can change the instruction and input - leave the output blank!

# In[16]:


inputs = tokenizer(
[
    alpaca_prompt.format(
        "Wskaż wszystkie czasowniki w zdaniu.", # instruction
        "Swoim rektorskim przemówieniem inauguracyjnym z 25 listopada 1862, w którym przypominał związki Polski z kulturą Zachodu, zyskał sobie ogromną popularność.", # input
        "", # output - leave this blank for generation!
    )
]*1, return_token_type_ids=False, return_tensors = "pt").to("cuda")

outputs = model.generate(**inputs, max_new_tokens = 128, use_cache = True)
tokenizer.batch_decode(outputs)


# <a name="Save"></a>
# ### Saving, loading finetuned models
# To save the final model, either use Huggingface's `push_to_hub` for an online save or `save_pretrained` for a local save.

# In[ ]:


model.save_pretrained("APT3-1B-finetuned-v1-12k") # Local saving
# model.push_to_hub("your_name/lora_model") # Online saving


# To save to `GGUF` / `llama.cpp`, or for model merging, use `model.merge_and_unload` first, then save the model. See this [issue](https://github.com/ggerganov/llama.cpp/issues/3097) on llama.cpp for more info.

# Now if you want to load the LoRA adapters we just saved, we can!

# In[3]:


from peft import PeftModel
model = PeftModel.from_pretrained(model, "APT3-1B-finetuned-v1")


# Finally, we can now do some inference on the loaded model.

# In[13]:


inputs = tokenizer(
[
    alpaca_prompt.format(
        "Napisz wypiedź na temat pralki.", # instruction
        "", # input
        "", # output
    )
]*1,return_token_type_ids=False, return_tensors = "pt").to("cuda")

outputs = model.generate(**inputs, max_new_tokens = 128, use_cache = True)
tokenizer.batch_decode(outputs)


# In[21]:


inputs = tokenizer(
[
    alpaca_prompt.format(
        "Powiedz mi jak czuje się człowiek przed śmiercią...", # instruction
        "", # input
        "", # output
    )
]*1,return_token_type_ids=False, return_tensors = "pt").to("cuda")

outputs = model.generate(**inputs, max_new_tokens = 128, use_cache = True)
tokenizer.batch_decode(outputs)


# In[ ]:


model = model.merge_and_unload()
model.save_pretrained("APT3-1B-finetuned-v1-full-12k") # Local saving


# In[4]:


get_ipython().system('pip3 install gradio==3.50')


# In[7]:


import os
print(os.path.join("."))


# In[9]:


import gradio as gr
import torch
import time
from transformers import LlamaForCausalLM, PreTrainedTokenizerFast, pipeline

MODEL_PATH = "APT3-1B-finetuned-v1-full-12k"
bf16=True

print(f"Start loading the model")

#from unsloth import FastLanguageModel
#load_in_4bit = False # Use 4bit quantization to reduce memory usage. Can be False.

#model, tokenizer = FastLanguageModel.from_pretrained(
#    model_name = MODEL_PATH,
#    max_seq_length = 2048,
#    dtype = torch.bfloat16,
#    load_in_4bit = load_in_4bit,
#    # token = "hf_...", # use one if using gated models like meta-llama/Llama-2-7b-hf
#)

tokenizer = PreTrainedTokenizerFast.from_pretrained(MODEL_PATH)
model = LlamaForCausalLM.from_pretrained(
    MODEL_PATH,
    torch_dtype=(torch.bfloat16 if bf16 else torch.float32),
    device_map="auto",
    max_memory={0:"23500MB"},
)
print(f"Model sucessfully loaded")
model = torch.compile(model)
print(f"Model compiled")

def generate_text(prompt, max_length, temperature, top_k, top_p):
    input_ids = tokenizer(prompt.strip(), return_tensors='pt', add_special_tokens=False).input_ids.to(model.device)
    start_time = time.time()
    output = model.generate(
        inputs=input_ids,
        max_new_tokens=max_length,
        temperature=temperature,
        top_k=top_k,
        do_sample=(temperature > 0),
        top_p=top_p,
        num_beams=1,
        bos_token_id=0,
        eos_token_id=1,
        pad_token_id=3,
        repetition_penalty=1.1
    )
    elapsed_time = time.time() - start_time
    decoded_output = tokenizer.decode(output[0])
    input_tokens_count = len(input_ids[0])
    input_chars_count = len(prompt)
    output_tokens_count = len(output[0])
    output_chars_count = len(decoded_output)
    gen_speed = output_tokens_count / elapsed_time
    print(f"Input tokens: {input_tokens_count} (chars: {input_chars_count}), Output tokens: {output_tokens_count} (chars: {output_chars_count}), Gen Time: {elapsed_time:.2f} secs ({gen_speed} toks/sec)")
    return decoded_output, input_tokens_count, input_chars_count, output_tokens_count, output_chars_count, gen_speed

demo = gr.Interface(
    fn=generate_text,
    inputs=[
        gr.Textbox(label="Input Text"),
        gr.Slider(1, 1000, step=1, value=100, label="Max Length"),
        gr.Slider(0.0, 2.0, step=0.1, value=0.8, label="Temperature"),
        gr.Slider(1, 400, step=1, value=200, label="Top K"),
        gr.Slider(0.0, 1.0, step=0.05, value=0.95, label="Top P")
    ],
    outputs=[
        gr.Textbox(label="Generated Text"),
        gr.Textbox(label="Input Tokens Count"),
        gr.Textbox(label="Input Characters Count"),
        gr.Textbox(label="Output Tokens Count"),
        gr.Textbox(label="Output Characters Count"),
        gr.Textbox(label="Generation speed in tokens per second"),
    ]
)

if __name__ == "__main__":
    demo.launch()


# In[5]:


from unsloth import FastLanguageModel
import torch
dtype = None # None for auto detection. Float16 for Tesla T4, V100, Bfloat16 for Ampere+
load_in_4bit = False # Use 4bit quantization to reduce memory usage. Can be False.

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name = "APT3-1B-finetuned-v1_full",
    max_seq_length = max_seq_length,
    dtype = dtype,
    load_in_4bit = load_in_4bit,
    # token = "hf_...", # use one if using gated models like meta-llama/Llama-2-7b-hf
)


# In[10]:


import gradio as gr
import torch
import time
from transformers import LlamaForCausalLM, PreTrainedTokenizerFast, pipeline

MODEL_PATH = 'Azurro/APT3-1B-Base'
bf16=False

print(f"Start loading the model")
tokenizer = PreTrainedTokenizerFast.from_pretrained(MODEL_PATH)
model = LlamaForCausalLM.from_pretrained(
    MODEL_PATH,
    torch_dtype=(torch.bfloat16 if bf16 else torch.float32),
    device_map="auto",
    max_memory={0:"23500MB"},
)
print(f"Model sucessfully loaded")
model = torch.compile(model)
print(f"Model compiled")

def generate_text(prompt, max_length, temperature, top_k, top_p):
    input_ids = tokenizer(prompt.strip(), return_tensors='pt', add_special_tokens=False).input_ids.to(model.device)
    start_time = time.time()
    output = model.generate(
        inputs=input_ids,
        max_new_tokens=max_length,
        temperature=temperature,
        top_k=top_k,
        do_sample=(temperature > 0),
        top_p=top_p,
        num_beams=1,
        bos_token_id=0,
        eos_token_id=1,
        pad_token_id=3,
        repetition_penalty=1.1
    )
    elapsed_time = time.time() - start_time
    decoded_output = tokenizer.decode(output[0])
    input_tokens_count = len(input_ids[0])
    input_chars_count = len(prompt)
    output_tokens_count = len(output[0])
    output_chars_count = len(decoded_output)
    gen_speed = output_tokens_count / elapsed_time
    print(f"Input tokens: {input_tokens_count} (chars: {input_chars_count}), Output tokens: {output_tokens_count} (chars: {output_chars_count}), Gen Time: {elapsed_time:.2f} secs ({gen_speed} toks/sec)")
    return decoded_output, input_tokens_count, input_chars_count, output_tokens_count, output_chars_count, gen_speed

demo = gr.Interface(
    fn=generate_text,
    inputs=[
        gr.Textbox(label="Input Text"),
        gr.Slider(1, 1000, step=1, value=100, label="Max Length"),
        gr.Slider(0.0, 2.0, step=0.1, value=0.8, label="Temperature"),
        gr.Slider(1, 400, step=1, value=200, label="Top K"),
        gr.Slider(0.0, 1.0, step=0.05, value=0.95, label="Top P")
    ],
    outputs=[
        gr.Textbox(label="Generated Text"),
        gr.Textbox(label="Input Tokens Count"),
        gr.Textbox(label="Input Characters Count"),
        gr.Textbox(label="Output Tokens Count"),
        gr.Textbox(label="Output Characters Count"),
        gr.Textbox(label="Generation speed in tokens per second"),
    ]
)

if __name__ == "__main__":
    demo.launch(share=True)


# And we're done! If you have any questions on Unsloth, we have a [Discord](https://discord.gg/u54VK8m8tk) channel! If you find any bugs or want to keep updated with the latest LLM stuff, or need help, join projects etc, feel free to join our Discord!
# <div class="align-center">
#   <a href="https://github.com/unslothai/unsloth"><img src="https://github.com/unslothai/unsloth/raw/main/images/unsloth%20new%20logo.png" width="110"></a>
#   <a href="https://discord.gg/u54VK8m8tk"><img src="https://github.com/unslothai/unsloth/raw/main/images/Discord.png" width="150"></a>
#   <a href="https://huggingface.co/docs/trl/main/en/index"><img src="https://github.com/huggingface/blog/blob/main/assets/133_trl_peft/thumbnail.png?raw=true" width="100"></a>
# </div>
