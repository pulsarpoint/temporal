package workflows

import (
	"sort"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

type translationMissItem struct {
	Category string
	Item     contracts.TranslationItem
}

func flattenTranslationMisses(missesByCategory map[string][]contracts.TranslationItem) ([]contracts.TranslationItem, map[string]translationMissItem) {
	categories := make([]string, 0, len(missesByCategory))
	for category := range missesByCategory {
		categories = append(categories, category)
	}
	sort.Strings(categories)

	items := []contracts.TranslationItem{}
	itemByID := map[string]translationMissItem{}
	for _, category := range categories {
		for _, item := range missesByCategory[category] {
			if item.Text == "" {
				continue
			}
			itemID := category + ":" + item.ID
			items = append(items, contracts.TranslationItem{
				ID:       itemID,
				Category: category,
				Text:     item.Text,
			})
			itemByID[itemID] = translationMissItem{
				Category: category,
				Item:     item,
			}
		}
	}
	return items, itemByID
}
