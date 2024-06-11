package couchbase.test.val;

import ai.djl.MalformedModelException;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import com.amazonaws.util.json.JSONException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;
import couchbase.test.docgen.WorkLoadSettings;

import java.awt.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Random;
import java.util.List;

public class Cars {

    private final WorkLoadSettings ws;
    Faker faker;
    private Random random;
    public String[] carColors = {"darker blue", "darker green", "green again", "darker purple", "darker pink",
            "cloudy blue", "dark pastel green", "dust", "electric lime", "fresh green", "light eggplant",
            "nasty green", "really light blue", "tea", "warm purple", "yellowish tan", "cement", "dark grass green",
            "dusty teal", "grey teal", "pinkish tan", "strong blue", "toxic green", "windows blue",
            "blue blue", "blue with a hint of purple", "booger", "bright sea green", "dark green blue",
            "deep turquoise", "green teal", "strong pink", "bland", "deep aqua", "lavender pink", "light moss green",
            "light seafoam green", "olive yellow", "pig pink", "deep lilac", "desert", "dusty lavender",
            "purpley grey", "purply", "candy pink", "light pastel green", "boring green", "kiwi green",
            "light grey green", "orange pink", "tea green", "very light brown", "egg shell",
            "eggplant purple", "powder pink", "reddish grey", "baby shit brown", "liliac", "stormy blue",
            "ugly brown", "custard", "darkish pink", "deep brown", "greenish beige", "manilla", "off blue",
            "battleship grey", "browny green", "bruise", "kelley green", "sickly yellow", "sunny yellow", "azul",
            "darkgreen", "green/yellow", "lichen", "light light green", "pale gold", "sun yellow", "tan green",
            "burple", "butterscotch", "toupe", "dark cream", "indian red", "light lavendar", "poison green",
            "baby puke green", "bright yellow green", "charcoal grey", "squash", "cinnamon", "light pea green",
            "radioactive green", "raw sienna", "baby purple", "cocoa", "light royal blue", "orangeish", "rust brown",
            "sand brown", "swamp", "tealish green", "burnt siena", "camo", "dusk blue", "fern", "old rose", "pale light green",
            "peachy pink", "rosy pink", "light bluish green", "light bright green", "light neon green", "light seafoam",
            "tiffany blue", "washed out green", "browny orange", "nice blue", "sapphire", "greyish teal", "orangey yellow",
            "parchment", "straw", "very dark brown", "terracota", "ugly blue", "clear blue", "creme", "foam green",
            "grey/green", "light gold", "seafoam blue", "topaz", "violet pink", "wintergreen", "yellow tan", "dark fuchsia",
            "indigo blue", "light yellowish green", "pale magenta", "rich purple", "sunflower yellow", "green/blue", "leather",
            "racing green", "vivid purple", "dark royal blue", "hazel", "muted pink", "booger green", "canary", "cool grey",
            "dark taupe", "darkish purple", "true green", "coral pink", "dark sage", "dark slate blue", "flat blue",
            "mushroom", "rich blue", "dirty purple", "greenblue", "icky green", "light khaki", "warm blue", "dark hot pink",
            "deep sea blue", "carmine", "dark yellow green", "pale peach", "plum purple", "golden rod", "neon red", "old pink",
            "very pale blue", "blood orange", "grapefruit", "sand yellow", "clay brown", "dark blue grey", "flat green",
            "light green blue", "warm pink", "dodger blue", "gross green", "ice", "metallic blue", "pale salmon", "sap green",
            "algae", "bluey grey", "greeny grey", "highlighter green", "light light blue", "light mint", "raw umber",
            "vivid blue", "deep lavender", "dull teal", "light greenish blue", "mud green", "pinky", "red wine", "shit green",
            "tan brown", "darkblue", "rosa", "lipstick", "pale mauve", "claret", "dandelion", "orangered", "poop green",
            "ruby", "dark", "greenish turquoise", "pastel red", "piss yellow", "bright cyan", "dark coral", "algae green",
            "darkish red", "reddy brown", "blush pink", "camouflage green", "lawn green", "putty", "vibrant blue", "dark sand",
            "purple/blue", "saffron", "twilight", "warm brown", "bluegrey", "bubble gum pink", "duck egg blue",
            "greenish cyan", "petrol", "royal", "butter", "dusty orange", "off yellow", "pale olive green", "orangish", "leaf",
            "light blue grey", "dried blood", "lightish purple", "rusty red", "lavender blue", "light grass green",
            "light mint green", "sunflower", "velvet", "brick orange", "lightish red", "pure blue", "twilight blue",
            "violet red", "yellowy brown", "carnation", "muddy yellow", "dark seafoam green", "deep rose", "dusty red",
            "grey/blue", "lemon lime", "purple/pink", "brown yellow", "purple brown", "wisteria", "banana yellow",
            "lipstick red", "water blue", "brown grey", "vibrant purple", "baby green", "barf green", "eggshell blue",
            "sandy yellow", "cool green", "pale", "blue/grey", "hot magenta", "greyblue", "purpley", "baby shit green",
            "brownish pink", "dark aquamarine", "diarrhea", "light mustard", "pale sky blue", "turtle green", "bright olive",
            "dark grey blue", "greeny brown", "lemon green", "light periwinkle", "seaweed green", "sunshine yellow",
            "ugly purple", "medium pink", "puke brown", "very light pink", "viridian", "bile", "faded yellow",
            "very pale green", "vibrant green", "bright lime", "spearmint", "light aquamarine", "light sage", "yellowgreen",
            "baby poo", "dark seafoam", "deep teal", "heather", "rust orange", "dirty blue", "fern green", "bright lilac",
            "weird green", "peacock blue", "avocado green", "faded orange", "grape purple", "hot green", "lime yellow",
            "mango", "shamrock", "bubblegum", "purplish brown", "vomit yellow", "pale cyan", "key lime", "tomato red",
            "lightgreen", "merlot", "night blue", "purpleish pink", "apple", "baby poop green", "green apple", "heliotrope",
            "yellow/green", "almost black", "cool blue", "leafy green", "mustard brown", "dusk", "dull brown", "frog green",
            "vivid green", "bright light green", "fluro green", "kiwi", "seaweed", "navy green", "ultramarine blue", "iris",
            "pastel orange", "yellowish orange", "perrywinkle", "tealish", "dark plum", "pear", "pinkish orange",
            "midnight purple", "light urple", "dark mint", "greenish tan", "light burgundy", "turquoise blue", "ugly pink",
            "sandy", "electric pink", "muted purple", "mid green", "greyish", "neon yellow", "banana", "carnation pink",
            "tomato", "sea", "muddy brown", "turquoise green", "buff", "fawn", "muted blue", "pale rose", "dark mint green",
            "amethyst", "blue/green", "chestnut", "sick green", "pea", "rusty orange", "stone", "rose red", "pale aqua",
            "deep orange", "earth", "mossy green", "grassy green", "pale lime green", "light grey blue", "pale grey",
            "asparagus", "blueberry", "purple red", "pale lime", "greenish teal", "caramel", "deep magenta", "light peach",
            "milk chocolate", "ocher", "off green", "purply pink", "lightblue", "dusky blue", "golden", "light beige",
            "butter yellow", "dusky purple", "french blue", "ugly yellow", "greeny yellow", "orangish red", "shamrock green",
            "orangish brown", "tree green", "deep violet", "gunmetal", "blue/purple", "cherry", "sandy brown", "warm grey",
            "dark indigo", "midnight", "bluey green", "grey pink", "soft purple", "blood", "brown red", "medium grey", "berry",
            "poo", "purpley pink", "light salmon", "snot", "easter purple", "light yellow green", "dark navy blue", "drab",
            "light rose", "rouge", "purplish red", "slime green", "baby poop", "irish green", "pink/purple", "dark navy",
            "greeny blue", "light plum", "pinkish grey", "dirty orange", "rust red", "pale lilac", "orangey red",
            "primary blue", "kermit green", "brownish purple", "murky green", "wheat", "very dark purple", "bottle green",
            "watermelon", "deep sky blue", "fire engine red", "yellow ochre", "pumpkin orange", "pale olive", "light lilac",
            "lightish green", "carolina blue", "mulberry", "shocking pink", "auburn", "bright lime green", "celadon",
            "pinkish brown", "poo brown", "bright sky blue", "celery", "dirt brown", "strawberry", "dark lime", "copper",
            "medium brown", "muted green", "robin's egg", "bright aqua", "bright lavender", "ivory", "very light purple",
            "light navy", "pink red", "olive brown", "poop brown", "mustard green", "ocean green", "very dark blue",
            "dusty green", "light navy blue", "minty green", "adobe", "barney", "jade green", "bright light blue",
            "light lime", "dark khaki", "orange yellow", "ocre", "maize", "faded pink", "british racing green", "sandstone",
            "mud brown", "light sea green", "robin egg blue", "aqua marine", "dark sea green", "soft pink", "orangey brown",
            "cherry red", "burnt yellow", "brownish grey", "camel", "purplish grey", "marine", "greyish pink",
            "pale turquoise", "pastel yellow", "bluey purple", "canary yellow", "faded red", "sepia", "coffee",
            "bright magenta", "mocha", "ecru", "purpleish", "cranberry", "darkish green", "brown orange", "dusky rose",
            "melon", "sickly green", "silver", "purply blue", "purpleish blue", "hospital green", "shit brown", "mid blue",
            "amber", "easter green", "soft blue", "cerulean blue", "golden brown", "bright turquoise", "red pink",
            "red purple", "greyish brown", "vermillion", "russet", "steel grey", "lighter purple", "bright violet",
            "prussian blue", "slate green", "dirty pink", "dark blue green", "pine", "yellowy green", "dark gold", "bluish",
            "darkish blue", "dull red", "pinky red", "bronze", "pale teal", "military green", "barbie pink", "bubblegum pink",
            "pea soup green", "dark mustard", "shit", "medium purple", "very dark green", "dirt", "dusky pink", "red violet",
            "lemon yellow", "pistachio", "dull yellow", "dark lime green", "denim blue", "teal blue", "lightish blue",
            "purpley blue", "light indigo", "swamp green", "brown green", "dark maroon", "hot purple", "dark forest green",
            "faded blue", "drab green", "light lime green", "snot green", "yellowish", "light blue green", "bordeaux",
            "light mauve", "ocean", "marigold", "muddy green", "dull orange", "steel", "electric purple", "fluorescent green",
            "yellowish brown", "blush", "soft green", "bright orange", "lemon", "purple grey", "acid green", "pale lavender",
            "violet blue", "light forest green", "burnt red", "khaki green", "cerise", "faded purple", "apricot",
            "dark olive green", "grey brown", "green grey", "true blue", "pale violet", "periwinkle blue", "light sky blue",
            "blurple", "green brown", "bluegreen", "bright teal", "brownish yellow", "pea soup", "forest", "barney purple",
            "ultramarine", "purplish", "puke yellow", "bluish grey", "dark periwinkle", "dark lilac", "reddish",
            "light maroon", "dusty purple", "terra cotta", "avocado", "marine blue", "teal green", "slate grey",
            "lighter green", "electric green", "dusty blue", "golden yellow", "bright yellow", "light lavender", "umber",
            "poop", "dark peach", "jungle green", "eggshell", "denim", "yellow brown", "dull purple", "chocolate brown",
            "wine red", "neon blue", "dirty green", "light tan", "ice blue", "cadet blue", "dark mauve", "very light blue",
            "grey purple", "pastel pink", "very light green", "dark sky blue", "evergreen", "dull pink", "aubergine",
            "mahogany", "reddish orange", "deep green", "vomit green", "purple pink", "dusty pink", "faded green",
            "camo green", "pinky purple", "pink purple", "brownish red", "dark rose", "mud", "brownish", "emerald green",
            "pale brown", "dull blue", "burnt umber", "medium green", "clay", "light aqua", "light olive green",
            "brownish orange", "dark aqua", "purplish pink", "dark salmon", "greenish grey", "jade", "ugly green",
            "dark beige", "emerald", "pale red", "light magenta", "sky", "light cyan", "yellow orange", "reddish purple",
            "reddish pink", "orchid", "dirty yellow", "orange red", "deep red", "orange brown", "cobalt blue", "neon pink",
            "rose pink", "greyish purple", "raspberry", "aqua green", "salmon pink", "tangerine", "brownish green",
            "red brown", "greenish brown", "pumpkin", "pine green", "charcoal", "baby pink", "cornflower", "blue violet",
            "chocolate", "greyish green", "scarlet", "green yellow", "dark olive", "sienna", "pastel purple", "terracotta",
            "aqua blue", "sage green", "blood red", "deep pink", "grass", "moss", "pastel blue", "bluish green", "green blue",
            "dark tan", "greenish blue", "pale orange", "vomit", "forrest green", "dark lavender", "dark violet",
            "purple blue", "dark cyan", "olive drab", "pinkish", "cobalt", "neon purple", "light turquoise", "apple green",
            "dull green", "wine", "powder blue", "off white", "electric blue", "dark turquoise", "blue purple", "azure",
            "bright red", "pinkish red", "cornflower blue", "light olive", "grape", "greyish blue", "purplish blue",
            "yellowish green", "greenish yellow", "medium blue", "dusty rose", "light violet", "midnight blue",
            "bluish purple", "red orange", "dark magenta", "greenish", "ocean blue", "coral", "cream", "reddish brown",
            "burnt sienna", "brick", "sage", "grey green", "white", "robin's egg blue", "moss green", "steel blue", "eggplant",
            "light yellow", "leaf green", "light grey", "puke", "pinkish purple", "sea blue", "pale purple", "slate blue",
            "blue grey", "hunter green", "fuchsia", "crimson", "pale yellow", "ochre", "mustard yellow", "light red",
            "cerulean", "pale pink", "deep blue", "rust", "light teal", "slate", "goldenrod", "dark yellow", "dark grey",
            "army green", "grey blue", "seafoam", "puce", "spring green", "dark orange", "sand", "pastel green", "mint",
            "light orange", "bright pink", "chartreuse", "deep purple", "dark brown", "taupe", "pea green", "puke green",
            "kelly green", "seafoam green", "blue green", "khaki", "burgundy", "dark teal", "brick red", "royal purple",
            "plum", "mint green", "gold", "baby blue", "yellow green", "bright purple", "dark red", "pale blue", "grass green",
            "navy", "aquamarine", "burnt orange", "neon green", "bright blue", "rose", "light pink", "mustard", "indigo",
            "lime", "sea green", "periwinkle", "dark pink", "olive green", "peach", "pale green", "light brown", "hot pink",
            "black", "lilac", "navy blue", "royal blue", "beige", "salmon", "olive", "maroon", "bright green", "dark purple",
            "mauve", "forest green", "aqua", "cyan", "tan", "dark blue", "lavender", "turquoise", "dark green", "violet",
            "light purple", "lime green", "grey", "sky blue", "yellow", "magenta", "light green", "orange", "teal",
            "light blue", "red", "brown", "pink", "blue", "green", "purple"};
    public String[] carColorsHex = {
            "#011288", "#087804", "#16d43f", "#5f1b6b", "#c4387f", "#acc2d9", "#56ae57", "#b2996e", "#a8ff04", "#69d84f",
            "#894585", "#70b23f", "#d4ffff", "#65ab7c", "#952e8f", "#fcfc81", "#a5a391", "#388004", "#4c9085", "#5e9b8a",
            "#d99b82", "#0c06f7", "#61de2a", "#3778bf", "#2242c7", "#533cc6", "#9bb53c", "#05ffa6", "#1f6357", "#017374",
            "#0cb577", "#ff0789", "#afa88b", "#08787f", "#dd85d7", "#a6c875", "#a7ffb5", "#c2b709", "#e78ea5", "#966ebd",
            "#ccad60", "#ac86a8", "#947e94", "#983fb2", "#ff63e9", "#b2fba5", "#63b365", "#8ee53f", "#b7e1a1", "#ff6f52",
            "#bdf8a3", "#d3b683", "#fffcc4", "#430541", "#ffb2d0", "#997570", "#ad900d", "#c48efd", "#507b9c", "#7d7103",
            "#fffd78", "#da467d", "#410200", "#c9d179", "#fffa86", "#5684ae", "#6b7c85", "#6f6c0a", "#7e4071", "#009337",
            "#d0e429", "#fff917", "#1d5dec", "#054907", "#b5ce08", "#8fb67b", "#c8ffb0", "#fdde6c", "#ffdf22", "#a9be70",
            "#6832e3", "#fdb147", "#c7ac7d", "#fff39a", "#850e04", "#efc0fe", "#40fd14", "#b6c406", "#9dff00", "#3c4142",
            "#f2ab15", "#ac4f06", "#c4fe82", "#2cfa1f", "#9a6200", "#ca9bf7", "#875f42", "#3a2efe", "#fd8d49", "#8b3103",
            "#cba560", "#698339", "#0cdc73", "#b75203", "#7f8f4e", "#26538d", "#63a950", "#c87f89", "#b1fc99", "#ff9a8a",
            "#f6688e", "#76fda8", "#53fe5c", "#4efd54", "#a0febf", "#7bf2da", "#bcf5a6", "#ca6b02", "#107ab0", "#2138ab",
            "#719f91", "#fdb915", "#fefcaf", "#fcf679", "#1d0200", "#cb6843", "#31668a", "#247afd", "#ffffb6", "#90fda9",
            "#86a17d", "#fddc5c", "#78d1b6", "#13bbaf", "#fb5ffc", "#20f986", "#ffe36e", "#9d0759", "#3a18b1", "#c2ff89",
            "#d767ad", "#720058", "#ffda03", "#01c08d", "#ac7434", "#014600", "#9900fa", "#02066f", "#8e7618", "#d1768f",
            "#96b403", "#fdff63", "#95a3a6", "#7f684e", "#751973", "#089404", "#ff6163", "#598556", "#214761", "#3c73a8",
            "#ba9e88", "#021bf9", "#734a65", "#23c48b", "#8fae22", "#e6f2a2", "#4b57db", "#d90166", "#015482", "#9d0216",
            "#728f02", "#ffe5ad", "#4e0550", "#f9bc08", "#ff073a", "#c77986", "#d6fffe", "#fe4b03", "#fd5956", "#fce166",
            "#b2713d", "#1f3b4d", "#699d4c", "#56fca2", "#fb5581", "#3e82fc", "#a0bf16", "#d6fffa", "#4f738e", "#ffb19a",
            "#5c8b15", "#54ac68", "#89a0b0", "#7ea07a", "#1bfc06", "#cafffb", "#b6ffbb", "#a75e09", "#152eff", "#8d5eb7",
            "#5f9e8f", "#63f7b4", "#606602", "#fc86aa", "#8c0034", "#758000", "#ab7e4c", "#030764", "#fe86a4", "#d5174e",
            "#fed0fc", "#680018", "#fedf08", "#fe420f", "#6f7c00", "#ca0147", "#1b2431", "#00fbb0", "#db5856", "#ddd618",
            "#41fdfe", "#cf524e", "#21c36f", "#a90308", "#6e1005", "#fe828c", "#4b6113", "#4da409", "#beae8a", "#0339f8",
            "#a88f59", "#5d21d0", "#feb209", "#4e518b", "#964e02", "#85a3b2", "#ff69af", "#c3fbf4", "#2afeb7", "#005f6a",
            "#0c1793", "#ffff81", "#f0833a", "#f1f33f", "#b1d27b", "#fc824a", "#71aa34", "#b7c9e2", "#4b0101", "#a552e6",
            "#af2f0d", "#8b88f8", "#9af764", "#a6fbb2", "#ffc512", "#750851", "#c14a09", "#fe2f4a", "#0203e2", "#0a437a",
            "#a50055", "#ae8b0c", "#fd798f", "#bfac05", "#3eaf76", "#c74767", "#b9484e", "#647d8e", "#bffe28", "#d725de",
            "#b29705", "#673a3f", "#a87dc2", "#fafe4b", "#c0022f", "#0e87cc", "#8d8468", "#ad03de", "#8cff9e", "#94ac02",
            "#c4fff7", "#fdee73", "#33b864", "#fff9d0", "#758da3", "#f504c9", "#77a1b5", "#8756e4", "#889717", "#c27e79",
            "#017371", "#9f8303", "#f7d560", "#bdf6fe", "#75b84f", "#9cbb04", "#29465b", "#696006", "#adf802", "#c1c6fc",
            "#35ad6b", "#fffd37", "#a442a0", "#f36196", "#947706", "#fff4f2", "#1e9167", "#b5c306", "#feff7f", "#cffdbc",
            "#0add08", "#87fd05", "#1ef876", "#7bfdc7", "#bcecac", "#bbf90f", "#ab9004", "#1fb57a", "#00555a", "#a484ac",
            "#c45508", "#3f829d", "#548d44", "#c95efb", "#3ae57f", "#016795", "#87a922", "#f0944d", "#5d1451", "#25ff29",
            "#d0fe1d", "#ffa62b", "#01b44c", "#ff6cb5", "#6b4247", "#c7c10c", "#b7fffa", "#aeff6e", "#ec2d01", "#76ff7b",
            "#730039", "#040348", "#df4ec8", "#6ecb3c", "#8f9805", "#5edc1f", "#d94ff5", "#c8fd3d", "#070d0d", "#4984b8",
            "#51b73b", "#ac7e04", "#4e5481", "#876e4b", "#58bc08", "#2fef10", "#2dfe54", "#0aff02", "#9cef43", "#18d17b",
            "#35530a", "#1805db", "#6258c4", "#ff964f", "#ffab0f", "#8f8ce7", "#24bca8", "#3f012c", "#cbf85f", "#ff724c",
            "#280137", "#b36ff6", "#48c072", "#bccb7a", "#a8415b", "#06b1c4", "#cd7584", "#f1da7a", "#ff0490", "#805b87",
            "#50a747", "#a8a495", "#cfff04", "#ffff7e", "#ff7fa7", "#ef4026", "#3c9992", "#886806", "#04f489", "#fef69e",
            "#cfaf7b", "#3b719f", "#fdc1c5", "#20c073", "#9b5fc0", "#0f9b8e", "#742802", "#9db92c", "#a4bf20", "#cd5909",
            "#ada587", "#be013c", "#b8ffeb", "#dc4d01", "#a2653e", "#638b27", "#419c03", "#b1ff65", "#9dbcd4", "#fdfdfe",
            "#77ab56", "#464196", "#990147", "#befd73", "#32bf84", "#af6f09", "#a0025c", "#ffd8b1", "#7f4e1e", "#bf9b0c",
            "#6ba353", "#f075e6", "#7bc8f6", "#475f94", "#f5bf03", "#fffeb6", "#fffd74", "#895b7b", "#436bad", "#d0c101",
            "#c6f808", "#f43605", "#02c14d", "#b25f03", "#2a7e19", "#490648", "#536267", "#5a06ef", "#cf0234", "#c4a661",
            "#978a84", "#1f0954", "#03012d", "#2bb179", "#c3909b", "#a66fb5", "#770001", "#922b05", "#7d7f7c", "#990f4b",
            "#8f7303", "#c83cb9", "#fea993", "#acbb0d", "#c071fe", "#ccfd7f", "#00022e", "#828344", "#ffc5cb", "#ab1239",
            "#b0054b", "#99cc04", "#937c00", "#019529", "#ef1de7", "#000435", "#42b395", "#9d5783", "#c8aca9", "#c87606",
            "#aa2704", "#e4cbff", "#fa4224", "#0804f9", "#5cb200", "#76424e", "#6c7a0e", "#fbdd7e", "#2a0134", "#044a05",
            "#fd4659", "#0d75f8", "#fe0002", "#cb9d06", "#fb7d07", "#b9cc81", "#edc8ff", "#61e160", "#8ab8fe", "#920a4e",
            "#fe02a2", "#9a3001", "#65fe08", "#befdb7", "#b17261", "#885f01", "#02ccfe", "#c1fd95", "#836539", "#fb2943",
            "#84b701", "#b66325", "#7f5112", "#5fa052", "#6dedfd", "#0bf9ea", "#c760ff", "#ffffcb", "#f6cefc", "#155084",
            "#f5054f", "#645403", "#7a5901", "#a8b504", "#3d9973", "#000133", "#76a973", "#2e5a88", "#0bf77d", "#bd6c48",
            "#ac1db8", "#2baf6a", "#26f7fd", "#aefd6c", "#9b8f55", "#ffad01", "#c69c04", "#f4d054", "#de9dac", "#05480d",
            "#c9ae74", "#60460f", "#98f6b0", "#8af1fe", "#2ee8bb", "#11875d", "#fdb0c0", "#b16002", "#f7022a", "#d5ab09",
            "#86775f", "#c69f59", "#7a687f", "#042e60", "#c88d94", "#a5fbd5", "#fffe71", "#6241c7", "#fffe40", "#d3494e",
            "#985e2b", "#a6814c", "#ff08e8", "#9d7651", "#feffca", "#98568d", "#9e003a", "#287c37", "#b96902", "#ba6873",
            "#ff7855", "#94b21c", "#c5c9c7", "#661aee", "#6140ef", "#9be5aa", "#7b5804", "#276ab3", "#feb308", "#8cfd7e",
            "#6488ea", "#056eee", "#b27a01", "#0ffef9", "#fa2a55", "#820747", "#7a6a4f", "#f4320c", "#a13905", "#6f828a",
            "#a55af4", "#ad0afd", "#004577", "#658d6d", "#ca7b80", "#005249", "#2b5d34", "#bff128", "#b59410", "#2976bb",
            "#014182", "#bb3f3f", "#fc2647", "#a87900", "#82cbb2", "#667c3e", "#fe46a5", "#fe83cc", "#94a617", "#a88905",
            "#7f5f00", "#9e43a2", "#062e03", "#8a6e45", "#cc7a8b", "#9e0168", "#fdff38", "#c0fa8b", "#eedc5b", "#7ebd01",
            "#3b5b92", "#01889f", "#3d7afd", "#5f34e7", "#6d5acf", "#748500", "#706c11", "#3c0008", "#cb00f5", "#002d04",
            "#658cbb", "#749551", "#b9ff66", "#9dc100", "#faee66", "#7efbb3", "#7b002c", "#c292a1", "#017b92", "#fcc006",
            "#657432", "#d8863b", "#738595", "#aa23ff", "#08ff08", "#9b7a01", "#f29e8e", "#6fc276", "#ff5b00", "#fdff52",
            "#866f85", "#8ffe09", "#eecffe", "#510ac9", "#4f9153", "#9f2305", "#728639", "#de0c62", "#916e99", "#ffb16d",
            "#3c4d03", "#7f7053", "#77926f", "#010fcc", "#ceaefa", "#8f99fb", "#c6fcff", "#5539cc", "#544e03", "#017a79",
            "#01f9c6", "#c9b003", "#929901", "#0b5509", "#a00498", "#2000b1", "#94568c", "#c2be0e", "#748b97", "#665fd1",
            "#9c6da5", "#c44240", "#a24857", "#825f87", "#c9643b", "#90b134", "#01386a", "#25a36f", "#59656d", "#75fd63",
            "#21fc0d", "#5a86ad", "#fec615", "#fffd01", "#dfc5fe", "#b26400", "#7f5e00", "#de7e5d", "#048243", "#ffffd4",
            "#3b638c", "#b79400", "#84597e", "#411900", "#7b0323", "#04d9ff", "#667e2c", "#fbeeac", "#d7fffe", "#4e7496",
            "#874c62", "#d5ffff", "#826d8c", "#ffbacd", "#d1ffbd", "#448ee4", "#05472a", "#d5869d", "#3d0734", "#4a0100",
            "#f8481c", "#02590f", "#89a203", "#e03fd8", "#d58a94", "#7bb274", "#526525", "#c94cbe", "#db4bda", "#9e3623",
            "#b5485d", "#735c12", "#9c6d57", "#028f1e", "#b1916e", "#49759c", "#a0450e", "#39ad48", "#b66a50", "#8cffdb",
            "#a4be5c", "#cb7723", "#05696b", "#ce5dae", "#c85a53", "#96ae8d", "#1fa774", "#7a9703", "#ac9362", "#01a049",
            "#d9544d", "#fa5ff7", "#82cafc", "#acfffc", "#fcb001", "#910951", "#fe2c54", "#c875c4", "#cdc50a", "#fd411e",
            "#9a0200", "#be6400", "#030aa7", "#fe019a", "#f7879a", "#887191", "#b00149", "#12e193", "#fe7b7c", "#ff9408",
            "#6a6e09", "#8b2e16", "#696112", "#e17701", "#0a481e", "#343837", "#ffb7ce", "#6a79f7", "#5d06e9", "#3d1c02",
            "#82a67d", "#be0119", "#c9ff27", "#373e02", "#a9561e", "#caa0ff", "#ca6641", "#02d8e9", "#88b378", "#980002",
            "#cb0162", "#5cac2d", "#769958", "#a2bffe", "#10a674", "#06b48b", "#af884a", "#0b8b87", "#ffa756", "#a2a415",
            "#154406", "#856798", "#34013f", "#632de9", "#0a888a", "#6f7632", "#d46a7e", "#1e488f", "#bc13fe", "#7ef4cc",
            "#76cd26", "#74a662", "#80013f", "#b1d1fc", "#ffffe4", "#0652ff", "#045c5a", "#5729ce", "#069af3", "#ff000d",
            "#f10c45", "#5170d7", "#acbf69", "#6c3461", "#5e819d", "#601ef9", "#b0dd16", "#cdfd02", "#2c6fbb", "#c0737a",
            "#d6b4fc", "#020035", "#703be7", "#fd3c06", "#960056", "#40a368", "#03719c", "#fc5a50", "#ffffc2", "#7f2b0a",
            "#b04e0f", "#a03623", "#87ae73", "#789b73", "#ffffff", "#98eff9", "#658b38", "#5a7d9a", "#380835", "#fffe7a",
            "#5ca904", "#d8dcd6", "#a5a502", "#d648d7", "#047495", "#b790d4", "#5b7c99", "#607c8e", "#0b4008", "#ed0dd9",
            "#8c000f", "#ffff84", "#bf9005", "#d2bd0a", "#ff474c", "#0485d1", "#ffcfdc", "#040273", "#a83c09", "#90e4c1",
            "#516572", "#fac205", "#d5b60a", "#363737", "#4b5d16", "#6b8ba4", "#80f9ad", "#a57e52", "#a9f971", "#c65102",
            "#e2ca76", "#b0ff9d", "#9ffeb0", "#fdaa48", "#fe01b1", "#c1f80a", "#36013f", "#341c02", "#b9a281", "#8eab12",
            "#9aae07", "#02ab2e", "#7af9ab", "#137e6d", "#aaa662", "#610023", "#014d4e", "#8f1402", "#4b006e", "#580f41",
            "#8fff9f", "#dbb40c", "#a2cffe", "#c0fb2d", "#be03fd", "#840000", "#d0fefe", "#3f9b0b", "#01153e", "#04d8b2",
            "#c04e01", "#0cff0c", "#0165fc", "#cf6275", "#ffd1df", "#ceb301", "#380282", "#aaff32", "#53fca1", "#8e82fe",
            "#cb416b", "#677a04", "#ffb07c", "#c7fdb5", "#ad8150", "#ff028d", "#000000", "#cea2fd", "#001146", "#0504aa",
            "#e6daa6", "#ff796c", "#6e750e", "#650021", "#01ff07", "#35063e", "#ae7181", "#06470c", "#13eac9", "#00ffff",
            "#d1b26f", "#00035b", "#c79fef", "#06c2ac", "#033500", "#9a0eea", "#bf77f6", "#89fe05", "#929591", "#75bbfd",
            "#ffff14", "#c20078", "#96f97b", "#f97306", "#029386", "#95d0fc", "#e50000", "#653700", "#ff81c0", "#0343df",
            "#15b01a", "#7e1e9c"
    };
    public String[] carManufacturer = {
            "Toyota", "Honda", "Ford", "Chevrolet", "Nissan", "Mercedes-Benz", "BMW", "Audi", "Volkswagen", "Hyundai",
            "Kia", "Subaru", "Lexus", "Mazda", "Jeep", "Volvo", "Tesla", "Porsche", "Land Rover", "Ferrari",
            "Mitsubishi", "Chrysler", "Dodge", "Buick", "Cadillac", "GMC", "Lincoln", "Acura", "Infiniti", "Jaguar",
            "Ram", "Mini", "Alfa Romeo", "Genesis", "Smart", "Bentley", "Maserati", "Fiat", "McLaren", "Rolls-Royce",
            "Bugatti", "Aston Martin", "Lamborghini", "Lotus", "Maybach", "Morgan", "Pagani", "Spyker", "TVR",
            "Daihatsu", "Datsun", "Hennessey", "Hummer", "Isuzu", "Koenigsegg", "Lancia", "MG", "Mosler", "Plymouth",
            "Pontiac", "Saturn", "Scion", "Shelby", "SsangYong", "Suzuki", "Tata", "Vector", "Wiesmann", "Zenvo",
            "Brabus", "Geely", "Holden", "Maruti", "Proton", "Roewe", "Tazzari", "Think", "Vauxhall", "Zagato",
            "DeLorean", "Fisker", "Great Wall", "Mahindra", "Perodua", "Qoros", "Ruf", "Tata", "Wuling", "Zotye"
    };
    public String[] carFuels = {"Gasoline", "Ethanol", "Electric", "Methanol", "LPG" , "CNG", "Diesel"};
    public Predictor<String, float[]> predictor = null;
    public String[] carModels = {
            "Camry", "Corolla", "RAV4", "Highlander", "Tacoma", "Tundra", "Prius", "Avalon", "4Runner", "Sienna",
            "Civic", "Accord", "CR-V", "Pilot", "Fit", "Odyssey", "HR-V", "Ridgeline", "Passport", "Insight",
            "F-150", "Mustang", "Explorer", "Escape", "Ranger", "Edge", "Expedition", "Bronco", "Fusion", "EcoSport",
            "Silverado", "Malibu", "Equinox", "Tahoe", "Traverse", "Colorado", "Camaro", "Blazer", "Impala", "Trax",
            "Altima", "Sentra", "Maxima", "Rogue", "Murano", "Pathfinder", "Frontier", "Titan", "Leaf", "Armada",
            "C-Class", "E-Class", "S-Class", "GLE", "GLC", "GLA", "CLS", "G-Class", "A-Class", "SL",
            "3 Series", "5 Series", "7 Series", "X3", "X5", "X7", "Z4", "M3", "M5", "i3",
            "A3", "A4", "A6", "A8", "Q3", "Q5", "Q7", "Q8", "R8", "TT",
            "Golf", "Jetta", "Passat", "Tiguan", "Atlas", "Beetle", "Arteon", "ID.4", "Polo", "Touareg",
            "Elantra", "Sonata", "Tucson", "Santa Fe", "Kona", "Veloster", "Palisade", "Venue", "Accent", "Ioniq",
            "Optima", "Sorento", "Sportage", "Soul", "Telluride", "Stinger", "Forte", "Seltos", "Carnival", "Rio",
            "Impreza", "Outback", "Forester", "Crosstrek", "Ascent", "Legacy", "WRX", "BRZ", "Baja", "SVX",
            "ES", "IS", "GS", "LS", "NX", "RX", "LX", "GX", "LC", "RC",
            "Mazda3", "Mazda6", "CX-3", "CX-5", "CX-9", "MX-5 Miata", "CX-30", "Mazda2", "Mazda5", "CX-7",
            "Wrangler", "Grand Cherokee", "Cherokee", "Compass", "Renegade", "Gladiator", "Patriot", "Commander", "Liberty", "Wagoneer",
            "XC90", "XC60", "XC40", "S60", "S90", "V60", "V90", "C40", "V40", "240",
            "Model S", "Model 3", "Model X", "Model Y", "Cybertruck", "Roadster", "Semi", "Model 2", "Model R", "Model Q",
            "911", "Cayenne", "Panamera", "Macan", "Taycan", "Boxster", "Cayman", "Carrera GT", "918 Spyder", "Targa",
            "Range Rover", "Discovery", "Defender", "Evoque", "Velar", "Sport", "Freelander", "LR4", "LR2", "Series III",
            "488", "812 Superfast", "Portofino", "F8 Tributo", "SF90 Stradale", "GTC4Lusso", "California", "LaFerrari", "Enzo", "458",
            "Outlander", "Eclipse Cross", "ASX", "Pajero", "Mirage", "Lancer", "Triton", "Galant", "3000GT", "Starion",
            "300", "Pacifica", "Voyager", "Aspen", "Crossfire", "Sebring", "Concorde", "LHS", "PT Cruiser", "Neon",
            "Charger", "Challenger", "Durango", "Ram 1500", "Journey", "Grand Caravan", "Viper", "Dart", "Neon", "Magnum",
            "Enclave", "Encore", "Envision", "LaCrosse", "Regal", "Verano", "Lucerne", "LeSabre", "Riviera", "Century",
            "Escalade", "CTS", "XT5", "XT4", "XT6", "ATS", "XTS", "SRX", "CT6", "DeVille",
            "Sierra", "Yukon", "Canyon", "Acadia", "Terrain", "Savanna", "Jimmy", "Envoy", "Safari", "Sonoma",
            "Navigator", "Aviator", "Corsair", "Nautilus", "MKZ", "Continental", "MKT", "MKX", "MKC", "Town Car",
            "MDX", "RDX", "TLX", "ILX", "NSX", "RLX", "ZDX", "TSX", "Integra", "Legend",
            "Q50", "QX60", "QX80", "QX50", "QX70", "Q60", "QX30", "Q70", "G35", "EX35",
            "F-Pace", "E-Pace", "XE", "XF", "XJ", "F-Type", "I-Pace", "XK", "X-Type", "S-Type",
            "1500", "2500", "3500", "4500", "5500", "ProMaster", "ProMaster City", "Rebel", "Laramie", "TRX",
            "Cooper", "Countryman", "Clubman", "Paceman", "Coupe", "Convertible", "Roadster", "John Cooper Works", "Electric", "Hardtop",
            "Giulia", "Stelvio", "4C", "Giulietta", "GTV", "Spider", "Milano", "Montreal", "164", "8C",
            "G70", "G80", "G90", "GV70", "GV80", "Essentia", "Mint", "New York Concept", "X Concept", "GV60",
            "Fortwo", "Forfour", "Roadster", "Crossblade", "Brabus", "Electric Drive", "Cabrio", "City-Coupé", "Pulse", "Pure",
            "Continental GT", "Bentayga", "Flying Spur", "Mulsanne", "Arnage", "Azure", "Brooklands", "Turbo R", "Eight", "T Series",
            "Ghibli", "Levante", "Quattroporte", "GranTurismo", "GranCabrio", "MC20", "Biturbo", "Merak", "Bora", "Indy",
            "500", "Panda", "Tipo", "Punto", "124 Spider", "500L", "500X", "Doblo", "Fiorino", "Uno",
            "720S", "570S", "650S", "P1", "Senna", "GT", "765LT", "600LT", "Elva", "MP4-12C",
            "Phantom", "Ghost", "Wraith", "Dawn", "Cullinan", "Silver Cloud", "Silver Shadow", "Corniche", "Silver Wraith", "Seraph",
            "Veyron", "Chiron", "Divo", "Centodieci", "La Voiture Noire", "Bolide", "EB110", "Type 35", "Type 57", "Galibier",
            "DB11", "Vantage", "DBS", "DBX", "Rapide", "Vanquish", "Valkyrie", "Valhalla", "One-77", "Cygnet",
            "Aventador", "Huracan", "Urus", "Gallardo", "Murcielago", "Diablo", "Countach", "Miura", "Sian", "Veneno",
            "Evora", "Exige", "Elise", "Esprit", "Emira", "Eletre", "Europa", "Elite", "Seven", "Evija",
            "57", "62", "S560", "S650", "G650 Landaulet", "S680", "S500", "S600", "Zeppelin", "SW38",
            "Plus Four", "Plus Six", "3 Wheeler", "4/4", "Roadster", "Aero 8", "Plus 8", "Super 3", "Eva GT", "Aero GT",
            "Zonda", "Huayra", "Imola", "C10", "R", "Cinque", "Tricolore", "BC", "Revolucion", "Monza",
            "C8", "C12", "C8 Laviolette", "C8 Aileron", "C12 Zagato", "C8 Double 12", "C8 Spyder", "C8 Preliator", "D8 Peking-to-Paris", "B6 Venator",
            "Griffith", "Tuscan", "Chimaera", "Cerbera", "Sagaris", "Tamora", "T350", "Vixen", "M Series", "Grantura",
            "Charade", "Terios", "Mira", "Rocky", "Sirion", "Tanto", "Copen", "Boon", "Wake", "Move",
            "240Z", "260Z", "280Z", "280ZX", "300ZX", "310", "200SX", "240SX", "350Z", "370Z",
            "Venom GT", "Venom F5", "Venom 800", "Venom 1000", "Goliath 6x6", "Mammoth 6x6", "HPE1000", "HPE1200", "Maximus", "Exorcist",
            "H1", "H2", "H3", "H3T", "H4", "HMMWV", "Humvee C-Series", "Humvee EV", "Humvee R", "Humvee Tactical",
            "Trooper", "Rodeo", "Axiom", "Ascender", "Vehicross", "Amigo", "Hombre", "Impulse", "Stylus", "i-Series",
            "CCX", "Agera", "Regera", "Jesko", "Gemera", "One:1", "Agera RS", "Agera R", "CCXR", "CCR",
            "Delta", "Stratos", "Ypsilon", "Thema", "Fulvia", "Beta", "Gamma", "Dedra", "Kappa", "Prisma",
            "MG3", "MG5", "MG6", "ZS", "HS", "GS", "RX5", "Marvel R", "EHS", "TF",
            "MT900", "Consulier", "Raptor", "Photon", "Land Shark", "Intruder", "Twinstar", "M-80", "J10", "M900S",
            "Barracuda", "Fury", "Duster", "Valiant", "Superbird", "Belvedere", "GTX", "Satellite", "Volare", "Concord",
            "GTO", "Firebird", "Trans Am", "Grand Prix", "Bonneville", "Catalina", "LeMans", "Fiero", "Solstice", "Torrent",
            "S-Series", "L-Series", "Vue", "Ion", "Sky", "Aura", "Relay", "Outlook", "SC", "SL",
            "FR-S", "tC", "xB", "xD", "iQ", "iM", "iA", "xA", "xC", "xD",
            "GT350", "GT500", "GT350R", "Cobra", "GT500KR", "GT-H", "CSX", "Series 1", "Daytona", "Series 2",
            "Korando", "Tivoli", "Rexton", "Musso", "Kyron", "Actyon", "Rodius", "Stavic", "Chairman", "Korando Sports",
            "Swift", "Vitara", "S-Cross", "Baleno", "Ignis", "Celerio", "Jimny", "Alto", "Ertiga", "XL7",
            "Nexon", "Harrier", "Safari", "Altroz", "Tiago", "Tigor", "Hexa", "Zest", "Bolt", "Nano",
            "W8", "W12", "W2", "M12", "M12 GT", "M12 GTS", "M12 GTR", "W2 TT", "W2 Twin Turbo", "M12 R",
            "MF3", "MF30", "MF4", "MF5", "MF3 Roadster", "GT MF5", "GT MF4", "Roadster MF5", "Spyder MF3", "GT MF3",
            "ST1", "TS1", "TSR", "ST1 GT", "TSR-S", "ST1 S", "TSR-GT", "ST1 R", "TSR-1", "TSR-2",
            "G500", "G700", "G800", "B63", "B65", "B50", "B40", "B35", "B25", "B20",
            "GC9", "Borui", "Vision", "Emgrand", "X7", "GC6", "EC7", "Panda", "Imperial", "CMA",
            "Commodore", "Ute", "Caprice", "Monaro", "Statesman", "Torana", "Kingswood", "Berlina", "Senator", "Cruze",
            "Swift", "Dzire", "Baleno", "Vitara Brezza", "Ertiga", "Alto", "Celerio", "Ignis", "S-Cross", "XL6",
            "Saga", "Persona", "Exora", "Iriz", "Suprima S", "Preve", "Waja", "Putra", "Satria", "Juara",
            "RX5", "950", "350", "E50", "550", "750", "i6", "350T", "750T", "350X",
            "Zero", "M", "CitySport", "Electric Sport", "M4", "M3", "Race", "SuperSport", "City", "City Fun",
            "City", "2", "Ampera-e", "Crossland", "Grandland", "Insignia", "Astra", "Combo", "Zafira", "Mokka",
            "IsoRivolta", "IsoGrifo", "IsoFidia", "IsoLele", "IsoNembo", "IsoMilano", "IsoChrono", "IsoItalia", "IsoArco", "IsoDino",
            "DMC-12", "DMC-24", "DMC-44", "EV1", "Alpha5", "E9", "GT", "EV", "E4", "E8",
            "Karma", "Ocean", "Atlantic", "Alaska", "PEAR", "E-Motion", "Sunrise", "Orbit", "Thunder", "Evo",
            "Haval H1", "Haval H2", "Haval H6", "Haval H9", "Steed 5", "Steed 6", "P Series", "Cannon", "Poer", "Tank 300",
            "Scorpio", "Bolero", "Thar", "XUV300", "XUV500", "XUV700", "KUV100", "Marazzo", "Alturas G4", "TUV300",
            "Axia", "Bezza", "Myvi", "Alza", "Aruz", "Kancil", "Kembara", "Kelisa", "Kenari", "Viva",
            "3", "5", "6", "7", "8", "9", "G", "T", "GT", "SUV",
            "CTR", "SCR", "RGT", "RT", "RT12", "R50", "R50S", "R52", "R55", "R57",
            "Nexon", "Harrier", "Safari", "Altroz", "Tiago", "Tigor", "Hexa", "Zest", "Bolt", "Nano",
            "Hongguang", "Cortez", "Almaz", "Formo", "Bajun", "Kiwi EV", "Victory", "Asta", "Star", "Jingchi",
            "T600", "SR7", "SR9", "T300", "Coupa", "Domy X5", "Domy X7", "T500", "T700", "Cloud 100"};
    public String [] carTransmissions = {"Manual", "Automatic", "Semi-Automatic",
            "CVT (Continuously Variable Transmission)", "Dual-Clutch Transmission", "Tiptronic",
            "DSG (Direct Shift Gearbox)", "AMT (Automated Manual Transmission)",
            "IVT (Intelligent Variable Transmission)", "eCVT (Electronic Continuously Variable Transmission)"
    };
    public String[] carCategories = {"Sedan", "SUV", "Truck", "Coupe", "Convertible", "Hatchback", "Minivan",
            "Van", "Wagon", "Crossover", "Hybrid", "Sports Car", "Luxury Car", "Compact", "Subcompact",
            "Pickup", "Roadster", "Supercar", "Muscle Car"};

    public String[] carModelVariants = {"LX", "Special Edition", "EX", "EX-L", "Touring", "L", "LE", "SE",
            "XLE", "XSE", "TRD", "320i", "330i", "340i", "M3", "530i", "540i", "550i", "M5", "Premium",
            "Premium Plus", "Prestige", "S4", "SE", "SEL", "SEL Plus", "N Line", "Limited",
            "LX", "S", "EX", "SX", "SX Prestige"};
    String[] safetyFeatures = {
            "Adaptive cruise control",
            "Lane departure warning",
            "Blind-spot monitoring",
            "Automatic emergency braking",
            "Rearview cameras",
            "Parking sensors",
            "Anti-lock Braking System (ABS)",
            "Electronic Stability Control (ESC)",
            "Traction Control System (TCS)",
            "Front airbags",
            "Side airbags",
            "Curtain airbags",
            "Tire Pressure Monitoring System (TPMS)",
            "Hill Start Assist"
    };

    String[] comfortAndConvenience = {
            "Keyless entry and ignition",
            "Heated and ventilated seats",
            "Dual-zone or tri-zone climate control",
            "Power-adjustable seats",
            "Panoramic sunroof"
    };

    String[] performanceAndEfficiency = {
            "Turbocharged engines",
            "Hybrid and electric powertrains",
            "Regenerative braking",
            "Drive mode selectors"
    };

    String[] interiorAndCargoSpace = {
            "Fold-flat rear seats",
            "Configurable storage options",
            "Premium materials (leather, wood trim)"
    };

    String[] smartCarFeatures = {
            "Autonomous driving capabilities",
            "Traffic sign recognition",
            "Head-up display",
            "Surround-view camera systems",
            "Adaptive headlights",
            "LED headlights and taillights",
            "Alloy wheels",
            "Power liftgate",
            "Roof rails",
            "Wi-Fi hotspot",
            "Wireless charging",
            "USB ports",
            "Remote start and vehicle monitoring via mobile apps",
            "Premium sound systems",
            "Rear-seat entertainment systems",
            "Satellite radio",
            "Ambient interior lighting",
            "Adaptive LED or Matrix LED headlights",
            "Touchscreen displays",
            "Apple CarPlay and Android Auto",
            "Bluetooth connectivity",
            "Integrated navigation systems"
    };
    public String carDescription = "This is a %s car with %s transmission and manufactured in %d year. " +
            "This car is available in %s color. This car belongs to %s category and has a rating of %d stars";

    public JsonArray hexToRgb(String hexCode) {
        Color color = Color.decode(hexCode);

        JsonArray rgb = JsonArray.create();
        rgb.add(color.getRed());
        rgb.add(color.getGreen());
        rgb.add(color.getBlue());
        return rgb;
    }
    public Cars(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.random = new Random();
        this.random.setSeed(ws.keyPrefix.hashCode());
        faker = new Faker();
        this.setEmbeddingsModel(ws.model);
    }

    public ArrayList<String> selectRandomItems(String[] array, int numberOfItems) {
        if (numberOfItems > array.length) {
            throw new IllegalArgumentException("Number of items to select cannot be greater than the length of the array");
        }
        // If the random value for numOfItems selected is 0 then return 1 item from the array
        if (numberOfItems == 0) {
            numberOfItems = 1;
        }

        ArrayList<String> selectedItems = new ArrayList<>();
        List<Integer> selectedIndices = new ArrayList<>();

        while (selectedItems.size() < numberOfItems) {
            int randomIndex = this.random.nextInt(array.length);
            if (!selectedIndices.contains(randomIndex)) {
                selectedIndices.add(randomIndex);
                selectedItems.add(array[randomIndex]);
            }
        }

        return selectedItems;
    }

    public JsonArray convertFloatVectorToJSON(float[] vector) throws JSONException {
        JsonArray jsonVal = JsonArray.create();
        for (float value: vector) jsonVal.add( value);
        return jsonVal;
    }

    // Function to reduce the dimensionality to 128
    private static float[] reduceTo128Dimensions(float[] embedding) {
        int targetDimension = 128;
        float[] reducedEmbedding = new float[targetDimension];
        int originalDimension = embedding.length;
        for (int i = 0; i < targetDimension; i++) {
            reducedEmbedding[i] = embedding[i % originalDimension];
        }
        return reducedEmbedding;
    }

    public void setEmbeddingsModel(String DJL_MODEL) {
        String DJL_PATH = "djl://ai.djl.huggingface.pytorch/" + DJL_MODEL;
        Criteria<String, float[]> criteria =
                Criteria.builder()
                        .setTypes(String.class, float[].class)
                        .optModelUrls(DJL_PATH)
                        .optEngine("PyTorch")
                        .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                        .optProgress(new ProgressBar())
                        .build();
        ZooModel<String, float[]> model = null;
        try {
            model = criteria.loadModel();
        } catch (ModelNotFoundException | MalformedModelException | IOException e) {
            e.printStackTrace();
        }
        this.predictor = model.newPredictor();
    }
    public static byte[] floatsToBytes(float[] floats) {
        byte bytes[] = new byte[Float.BYTES * floats.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);

        return bytes;
    }
    public static String convertToBase64Bytes(float[] floats) {
        return Base64.getEncoder().encodeToString(floatsToBytes(floats));
    }
    public ArrayList<JsonObject> getCarEvaluation(){
        int numReviews = this.random.nextInt(5);
        ArrayList<JsonObject> temp = new ArrayList<>();
        JsonArray vectorArrays = JsonArray.create();
        for (int n = 0; n <= numReviews; n++) {
            JsonObject evaluation = JsonObject.create();
            ArrayList<String> safety = selectRandomItems(safetyFeatures, this.random.nextInt(5));
            ArrayList<String> comfort = selectRandomItems(comfortAndConvenience, this.random.nextInt(5));
            ArrayList<String> interior = selectRandomItems(interiorAndCargoSpace, this.random.nextInt(5));
            ArrayList<String> performance = selectRandomItems(performanceAndEfficiency, this.random.nextInt(5));
            ArrayList<String> smartFeatures = selectRandomItems(smartCarFeatures, this.random.nextInt(5));
            float[] safetyVector = new float[0];
            float[] smartFeaturesVector = new float[0];
            float[] performanceVector = new float[0];
            float[] interiorVector = new float[0];
            float[] comfortVector = new float[0];
            try {
                safetyVector = this.predictor.predict(safety.toString());
                smartFeaturesVector = this.predictor.predict(smartFeatures.toString());
                performanceVector = this.predictor.predict(performance.toString());
                interiorVector = this.predictor.predict(interior.toString());
                comfortVector = this.predictor.predict(comfort.toString());

                safetyVector = reduceTo128Dimensions(safetyVector);
                smartFeaturesVector = reduceTo128Dimensions(smartFeaturesVector);
                performanceVector = reduceTo128Dimensions(performanceVector);
                interiorVector = reduceTo128Dimensions(interiorVector);
                comfortVector = reduceTo128Dimensions(comfortVector);

            } catch (TranslateException e) {
                e.printStackTrace();
            }
            if (this.ws.base64) {
                vectorArrays.add(convertToBase64Bytes(interiorVector));
                vectorArrays.add(convertToBase64Bytes(comfortVector));
                vectorArrays.add(convertToBase64Bytes(performanceVector));
                vectorArrays.add(convertToBase64Bytes(smartFeaturesVector));
                vectorArrays.add(convertToBase64Bytes(safetyVector));
                evaluation.put("featureVector", vectorArrays);

            } else {
                try {
                    vectorArrays.add(convertFloatVectorToJSON(interiorVector));
                    vectorArrays.add(convertFloatVectorToJSON(comfortVector));
                    vectorArrays.add(convertFloatVectorToJSON(performanceVector));
                    vectorArrays.add(convertFloatVectorToJSON(smartFeaturesVector));
                    vectorArrays.add(convertFloatVectorToJSON(safetyVector));
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
                evaluation.put("featureVectors", vectorArrays);
            }
            String variant = carModelVariants[this.random.nextInt(carModelVariants.length)];
            evaluation.put("variant", variant);
            evaluation.put("safety",safety );
            evaluation.put("comfort", comfort);
            evaluation.put("interior", interior);
            evaluation.put("peformance", performance);
            evaluation.put("smart features", smartFeatures);
            temp.add(evaluation);
        }
        return temp;
    }

    public JsonObject next(String key) {
        this.random = new Random();
        JsonObject jsonObject = JsonObject.create();
        this.random.setSeed(key.hashCode());
        int id = this.random.nextInt();
        int rating = this.random.nextInt(5);
        int year = 1900 + this.random.nextInt(150);
        int price = 10000 + this.random.nextInt(15000000);
        int colorPick = this.random.nextInt(carColors.length);
        JsonArray colorRGB = hexToRgb(carColorsHex[colorPick]);
        String manufacturer = carManufacturer[this.random.nextInt(carCategories.length)];
        String model = carModels[this.random.nextInt(carModels.length)];
        String fuel = carFuels[this.random.nextInt(carFuels.length)];
        String transmission = carTransmissions[this.random.nextInt(carTransmissions.length)];
        String category = carCategories[this.random.nextInt(carCategories.length)];
        String color = carColors[colorPick];
        String colorHex = carColorsHex[colorPick];
        String description = String.format(carDescription, manufacturer, transmission, year, color, category, rating);
        ArrayList<JsonObject> evaluation = getCarEvaluation();
        float[] descriptionVector = new float[0];
        if (this.ws.base64) {

        }
        try {
            descriptionVector = this.predictor.predict(description);
        } catch (TranslateException e) {
            e.printStackTrace();
        }
        if (this.ws.base64){
            String descriptionVectorJson = String.valueOf(convertToBase64Bytes(descriptionVector));
            jsonObject.put("descriptionVector", descriptionVectorJson);
        } else {
            JsonArray descriptionVectorJson;
            try {
                descriptionVectorJson = convertFloatVectorToJSON(descriptionVector);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            jsonObject.put("descriptionVector", descriptionVectorJson);
        }
        jsonObject.put("id", id);
        jsonObject.put("manufacturer", manufacturer);
        jsonObject.put("model", model);
        jsonObject.put("fuel", fuel);
        jsonObject.put("rating", rating);
        jsonObject.put("year", year);
        jsonObject.put("transmission", transmission);
        jsonObject.put("category", category);
        jsonObject.put("price", price);
        jsonObject.put("color", color);
        jsonObject.put("colorHex", colorHex);
        jsonObject.put("colorRGBVector", colorRGB);
        jsonObject.put("description", description);
        jsonObject.put("evaluation", evaluation);

        return jsonObject;
    }

}
