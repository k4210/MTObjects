#pragma once

#include <unordered_set>
#include <vector>
#include <algorithm>
#include <memory>
#include <assert.h>

namespace MTObjects
{
using std::unordered_set;
using std::vector;
using std::shared_ptr;
using std::weak_ptr;

struct IThreadSafeObject
{
private:
	friend struct Cluster;
	Cluster* cluster_ = nullptr;

public:
	virtual vector<IThreadSafeObject*>	IsDependentOn()	const = 0;
	virtual void IsConstDependentOn(unordered_set<const	IThreadSafeObject*>& ref_dependencies) const = 0;
};

struct Cluster : public std::enable_shared_from_this<Cluster>
{
	unordered_set<IThreadSafeObject*> objects_;
	unordered_set<const IThreadSafeObject*> const_dependencies_;
	unordered_set<const Cluster*> const_dependencies_clusters_;

	bool DependsOn(const Cluster* cluster) const
	{
		return const_dependencies_clusters_.end() != const_dependencies_clusters_.find(cluster);
	}
private:

	void GatherObjects(IThreadSafeObject* obj, unordered_set<IThreadSafeObject*>& all_objects, unordered_set<Cluster*> clusters_to_merge)
	{
		assert(obj);
		auto iter_all = all_objects.find(obj);
		const bool not_yet_in_cluster = iter_all != all_objects.end();
		assert(not_yet_in_cluster || obj->cluster_);

		if(not_yet_in_cluster)
		{
			const bool was_added = objects_.emplace(obj).second;
			assert(was_added);
			assert(nullptr == obj->cluster_);
			obj->cluster_ = this;
			all_objects.erase(iter_all);

			obj->IsConstDependentOn(const_dependencies_);

			auto dependencies = obj->IsDependentOn();
			for (IThreadSafeObject* dep_obj : dependencies)
			{
				GatherObjects(dep_obj, all_objects, clusters_to_merge);
			}
		}
		else if(obj->cluster_ != this)
		{
			clusters_to_merge.emplace(obj->cluster_);
		}
	}

	void MergeTo(Cluster& main_cluster)
	{
		for (auto obj : objects_)
		{
			obj->cluster_ = &main_cluster;
		}
		main_cluster.objects_.insert(objects_.begin(), objects_.end());
		main_cluster.const_dependencies_.insert(const_dependencies_.begin(), const_dependencies_.end());

		//At this stage const_dependencies_clusters_ are not generated yet
	}

	static void ClearClustersInObjects(const unordered_set<IThreadSafeObject*>& all_objects)
	{
		for (auto obj : all_objects)
		{
			obj->cluster_ = nullptr;
		}
	}

	static void CreateClusters(unordered_set<IThreadSafeObject *> &all_objects, unordered_set<shared_ptr<Cluster>> &clusters)
	{
		unordered_set<Cluster*> clusters_to_merge;
		clusters_to_merge.reserve(clusters.size());
		while (!all_objects.empty())
		{
			auto cluster = std::make_shared<Cluster>();
			cluster->GatherObjects(*all_objects.begin(), all_objects, clusters_to_merge);
			if (clusters_to_merge.empty())
			{
				clusters.emplace(cluster);
			}
			else
			{
				Cluster* main_cluster = *clusters_to_merge.begin();
				clusters_to_merge.erase(clusters_to_merge.begin());

				cluster->MergeTo(*main_cluster);

				for (auto other_cluster : clusters_to_merge)
				{
					other_cluster->MergeTo(*main_cluster);
					clusters.erase(other_cluster->shared_from_this());
				}
				clusters_to_merge.clear();
			}
		}
	}

	static void CreateClustersDependencies(const unordered_set<shared_ptr<Cluster>>& clusters)
	{
		for (auto& cluster : clusters)
		{
			for (const IThreadSafeObject* obj : cluster->const_dependencies_)
			{
				if (obj->cluster_ != cluster.get())
				{
					cluster->const_dependencies_clusters_.emplace(obj->cluster_);
				}
			}
		}
	}

public:
	static unordered_set<shared_ptr<Cluster>> GenerateClasters(unordered_set<IThreadSafeObject*> all_objects)
	{
		ClearClustersInObjects(all_objects);

		unordered_set<shared_ptr<Cluster>> clusters;
		CreateClusters(all_objects, clusters);
		CreateClustersDependencies(clusters);
		return clusters;
	}
};

struct GroupOfConcurrentClusters : public vector<shared_ptr<Cluster>>
{
	static vector<GroupOfConcurrentClusters> GenerateClasterGroups(const unordered_set<shared_ptr<Cluster>>& clusters)
	{
		auto fits_into_group = [](const GroupOfConcurrentClusters& group, const Cluster* cluster) -> bool
		{
			return std::none_of(group.begin(), group.end(), [&](const shared_ptr<Cluster>& cluster_in_group)
			{
				return cluster_in_group->DependsOn(cluster) || cluster->DependsOn(cluster_in_group.get());
			});
		};

		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(clusters.size());
		for (auto& cluster : clusters)
		{
			bool fits_in_existing_group = false;
			for (auto& group : groups)
			{
				if (fits_into_group(group, cluster.get()))
				{
					group.emplace_back(cluster);
					fits_in_existing_group = true;
					break;
				}
			}
			if (!fits_in_existing_group)
			{
				GroupOfConcurrentClusters new_group;
				new_group.emplace_back(cluster);
				groups.emplace_back(new_group);
			}
		}
		return groups;
	}
};
}